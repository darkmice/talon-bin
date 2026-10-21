//! Versioned native JSON ABI. Only bytes and opaque IDs cross the library boundary.
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::Duration,
};

const MAX_MESSAGE: usize = 8 * 1024 * 1024;
type Call = unsafe extern "C" fn(*const u8, usize, *mut *mut std::ffi::c_char) -> i32;
type Free = unsafe extern "C" fn(*mut std::ffi::c_char);

#[derive(Debug, Clone, thiserror::Error)]
#[error("{code}: {message}")]
pub struct Error {
    pub code: String,
    pub message: String,
}
pub fn error(message: impl Into<String>) -> Error {
    Error {
        code: "native_contract".into(),
        message: message.into(),
    }
}
impl From<serde_json::Error> for Error {
    fn from(_: serde_json::Error) -> Self {
        error("Invalid native wire value")
    }
}

struct Native {
    _library: libloading::Library,
    call: Call,
    free: Free,
    path: PathBuf,
    sha: String,
}
static NATIVE: OnceLock<Native> = OnceLock::new();
static LOAD: Mutex<()> = Mutex::new(());

/// Caller supplies the library admitted by its pinned release policy. Never
/// searches system library paths, downloads sources, or accepts an unpinned DLL.
pub fn configure(path: &Path, sha: &str) -> Result<(), Error> {
    let _guard = LOAD
        .lock()
        .map_err(|_| error("Library initialization poisoned"))?;
    let path = path
        .canonicalize()
        .map_err(|_| error("Native library is missing"))?;
    if let Some(native) = NATIVE.get() {
        return if native.path == path && native.sha == sha {
            Ok(())
        } else {
            Err(error("A different native runtime is already loaded"))
        };
    }
    if sha.len() != 64
        || !sha
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(error("A fixed SHA256 is required"));
    }
    let bytes = std::fs::read(&path).map_err(|_| error("Cannot read native library"))?;
    if format!("{:x}", Sha256::digest(&bytes)) != sha {
        return Err(error("Native library SHA256 mismatch"));
    }
    // SAFETY: hash-admitted producer; symbols have the versioned byte ABI below.
    let library = unsafe { libloading::Library::new(&path) }
        .map_err(|_| error("Cannot load native library"))?;
    let call = unsafe {
        *library
            .get::<Call>(b"talon_runtime_call_v1\0")
            .map_err(|_| error("Runtime v1 ABI is missing"))?
    };
    let free = unsafe {
        *library
            .get::<Free>(b"talon_runtime_free_v1\0")
            .map_err(|_| error("Runtime v1 free ABI is missing"))?
    };
    let candidate = Native {
        _library: library,
        call,
        free,
        path,
        sha: sha.into(),
    };
    let identity = invoke(&candidate, json!({"action":"identity"}))?;
    if identity["protocolVersion"] != 1 || identity["profile"] != "platform-host-v1" {
        return Err(error("Unsupported native runtime protocol"));
    }
    NATIVE
        .set(candidate)
        .map_err(|_| error("Native runtime already configured"))?;
    Ok(())
}

fn native() -> Result<&'static Native, Error> {
    if NATIVE.get().is_none() {
        let path = std::env::var_os("TALON_RUNTIME_LIBRARY")
            .ok_or_else(|| error("TALON_RUNTIME_LIBRARY is required"))?;
        let sha = std::env::var("TALON_RUNTIME_SHA256")
            .map_err(|_| error("TALON_RUNTIME_SHA256 is required"))?;
        configure(Path::new(&path), &sha)?;
    }
    NATIVE
        .get()
        .ok_or_else(|| error("Native runtime was not configured"))
}

pub fn call_json(request: Value) -> Result<Value, Error> {
    invoke(native()?, request)
}
fn invoke(native: &Native, request: Value) -> Result<Value, Error> {
    let input = serde_json::to_vec(&request)?;
    if input.len() > MAX_MESSAGE {
        return Err(error("Native request exceeds 8MiB"));
    }
    let mut output = std::ptr::null_mut();
    let code = unsafe { (native.call)(input.as_ptr(), input.len(), &mut output) };
    if output.is_null() {
        return Err(error("Native call returned no envelope"));
    }
    // Output belongs to this exact loaded library and is released even on decode failure.
    let envelope = unsafe {
        let bytes = std::ffi::CStr::from_ptr(output).to_bytes();
        let decoded = if bytes.len() <= MAX_MESSAGE {
            serde_json::from_slice::<Value>(bytes).map_err(Error::from)
        } else {
            Err(error("Native response exceeds 8MiB"))
        };
        (native.free)(output);
        decoded?
    };
    if code != 0 || envelope["ok"] != true {
        return Err(Error {
            code: envelope["code"]
                .as_str()
                .unwrap_or("native_contract")
                .into(),
            message: envelope["message"]
                .as_str()
                .unwrap_or("Native operation failed")
                .into(),
        });
    }
    Ok(envelope["data"].clone())
}

pub struct Operation {
    pub id: u64,
    pending: Mutex<VecDeque<Value>>,
}
impl Operation {
    pub fn start(mut request: Value) -> Result<Self, Error> {
        request["action"] = json!("start");
        let result = call_json(request)?;
        Ok(Self {
            id: result["id"]
                .as_u64()
                .ok_or_else(|| error("Missing operation ID"))?,
            pending: Mutex::new(VecDeque::new()),
        })
    }
    pub fn control(&self, action: &str, mut request: Value) -> Result<Value, Error> {
        request["action"] = json!(action);
        request["id"] = json!(self.id);
        call_json(request)
    }
    pub fn reply(&self, callback: u64, value: Value, done: bool) -> Result<(), Error> {
        self.control(
            "reply",
            json!({"callbackId":callback,"value":value,"done":done}),
        )
        .map(|_| ())
    }
    pub async fn reply_async(&self, callback: u64, value: Value, done: bool) -> Result<(), Error> {
        loop {
            match self.reply(callback, value.clone(), done) {
                Err(error) if error.code == "callback_busy" => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                result => return result,
            }
        }
    }
    pub fn cancel(&self) {
        let _ = self.control("cancel", json!({}));
    }
    pub async fn next(&self) -> Result<Value, Error> {
        loop {
            if let Some(event) = self
                .pending
                .lock()
                .map_err(|_| error("Event queue poisoned"))?
                .pop_front()
            {
                return Ok(event);
            }
            let batch = self.control("poll", json!({}))?;
            let batch = batch
                .as_array()
                .ok_or_else(|| error("Invalid event batch"))?;
            if !batch.is_empty() {
                self.pending
                    .lock()
                    .map_err(|_| error("Event queue poisoned"))?
                    .extend(batch.iter().cloned());
                continue;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        let _ = self.control("close", json!({}));
    }
}
