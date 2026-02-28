//! FFI bindings to Talon — AI-native multi-model data engine.
//!
//! This crate provides safe Rust wrappers around the Talon C ABI.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use talon_sys::Talon;
//!
//! let db = Talon::open("./my-data").expect("Failed to open database");
//!
//! // SQL
//! let result = db.run_sql("CREATE TABLE users (id INT, name TEXT)").unwrap();
//!
//! // KV
//! db.kv_set(b"key", b"value", 0).unwrap();
//! let val = db.kv_get(b"key").unwrap();
//!
//! // Vector search
//! db.vector_insert("embeddings", 1, &[0.1, 0.2, 0.3]).unwrap();
//! let results = db.vector_search("embeddings", &[0.1, 0.2, 0.3], 10, "cosine").unwrap();
//! ```

use std::ffi::{CStr, CString};
use std::fmt;
use std::ptr;
use std::slice;

/// Raw FFI bindings matching `talon.h`
mod ffi {
    use std::os::raw::{c_char, c_int};

    #[repr(C)]
    pub struct TalonHandle {
        _opaque: [u8; 0],
    }

    extern "C" {
        pub fn talon_open(path: *const c_char) -> *mut TalonHandle;
        pub fn talon_close(handle: *mut TalonHandle);

        pub fn talon_run_sql(
            handle: *const TalonHandle,
            sql: *const c_char,
            out_json: *mut *mut c_char,
        ) -> c_int;

        pub fn talon_kv_set(
            handle: *const TalonHandle,
            key: *const u8,
            key_len: usize,
            value: *const u8,
            value_len: usize,
            ttl_secs: i64,
        ) -> c_int;

        pub fn talon_kv_get(
            handle: *const TalonHandle,
            key: *const u8,
            key_len: usize,
            out_value: *mut *mut u8,
            out_len: *mut usize,
        ) -> c_int;

        pub fn talon_kv_del(
            handle: *const TalonHandle,
            key: *const u8,
            key_len: usize,
        ) -> c_int;

        pub fn talon_kv_incrby(
            handle: *const TalonHandle,
            key: *const u8,
            key_len: usize,
            delta: i64,
            out_value: *mut i64,
        ) -> c_int;

        pub fn talon_kv_setnx(
            handle: *const TalonHandle,
            key: *const u8,
            key_len: usize,
            value: *const u8,
            value_len: usize,
            ttl_secs: i64,
            was_set: *mut c_int,
        ) -> c_int;

        pub fn talon_vector_insert(
            handle: *const TalonHandle,
            index_name: *const c_char,
            id: u64,
            vec_data: *const f32,
            vec_dim: usize,
        ) -> c_int;

        pub fn talon_vector_search(
            handle: *const TalonHandle,
            index_name: *const c_char,
            vec_data: *const f32,
            vec_dim: usize,
            k: usize,
            metric: *const c_char,
            out_json: *mut *mut c_char,
        ) -> c_int;

        pub fn talon_persist(handle: *const TalonHandle) -> c_int;

        pub fn talon_execute(
            handle: *const TalonHandle,
            cmd_json: *const c_char,
            out_json: *mut *mut c_char,
        ) -> c_int;

        pub fn talon_free_string(ptr: *mut c_char);
        pub fn talon_free_bytes(ptr: *mut u8, len: usize);
    }
}

/// Error type for Talon operations.
#[derive(Debug)]
pub enum Error {
    /// Failed to open the database.
    Open(String),
    /// A Talon FFI call returned an error.
    Ffi(&'static str),
    /// Input string contained interior NUL byte.
    Nul(std::ffi::NulError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Open(path) => write!(f, "Failed to open Talon database at: {path}"),
            Error::Ffi(op) => write!(f, "Talon FFI call failed: {op}"),
            Error::Nul(e) => write!(f, "String contains interior NUL byte: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::ffi::NulError> for Error {
    fn from(e: std::ffi::NulError) -> Self {
        Error::Nul(e)
    }
}

/// A Talon database handle. Automatically closes on drop.
pub struct Talon {
    handle: *mut ffi::TalonHandle,
}

// SAFETY: TalonHandle is internally synchronized via Talon's storage engine.
unsafe impl Send for Talon {}
unsafe impl Sync for Talon {}

impl Talon {
    /// Open a Talon database at the given path.
    pub fn open(path: &str) -> Result<Self, Error> {
        let c_path = CString::new(path)?;
        let handle = unsafe { ffi::talon_open(c_path.as_ptr()) };
        if handle.is_null() {
            return Err(Error::Open(path.to_string()));
        }
        Ok(Talon { handle })
    }

    /// Execute a SQL statement. Returns the result as a JSON string.
    pub fn run_sql(&self, sql: &str) -> Result<String, Error> {
        let c_sql = CString::new(sql)?;
        let mut out: *mut std::os::raw::c_char = ptr::null_mut();
        let rc = unsafe { ffi::talon_run_sql(self.handle, c_sql.as_ptr(), &mut out) };
        if rc != 0 {
            return Err(Error::Ffi("run_sql"));
        }
        let result = unsafe { CStr::from_ptr(out).to_string_lossy().into_owned() };
        unsafe { ffi::talon_free_string(out) };
        Ok(result)
    }

    /// Set a key-value pair with optional TTL (0 = no expiry).
    pub fn kv_set(&self, key: &[u8], value: &[u8], ttl_secs: i64) -> Result<(), Error> {
        let rc = unsafe {
            ffi::talon_kv_set(
                self.handle,
                key.as_ptr(),
                key.len(),
                value.as_ptr(),
                value.len(),
                ttl_secs,
            )
        };
        if rc != 0 {
            return Err(Error::Ffi("kv_set"));
        }
        Ok(())
    }

    /// Get a value by key. Returns `None` if the key does not exist.
    pub fn kv_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let mut out_ptr: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let rc = unsafe {
            ffi::talon_kv_get(self.handle, key.as_ptr(), key.len(), &mut out_ptr, &mut out_len)
        };
        if rc != 0 {
            return Err(Error::Ffi("kv_get"));
        }
        if out_ptr.is_null() {
            return Ok(None);
        }
        let data = unsafe { slice::from_raw_parts(out_ptr, out_len).to_vec() };
        unsafe { ffi::talon_free_bytes(out_ptr, out_len) };
        Ok(Some(data))
    }

    /// Delete a key.
    pub fn kv_del(&self, key: &[u8]) -> Result<(), Error> {
        let rc = unsafe { ffi::talon_kv_del(self.handle, key.as_ptr(), key.len()) };
        if rc != 0 {
            return Err(Error::Ffi("kv_del"));
        }
        Ok(())
    }

    /// Atomic increment. Returns the new value.
    pub fn kv_incrby(&self, key: &[u8], delta: i64) -> Result<i64, Error> {
        let mut out: i64 = 0;
        let rc = unsafe {
            ffi::talon_kv_incrby(self.handle, key.as_ptr(), key.len(), delta, &mut out)
        };
        if rc != 0 {
            return Err(Error::Ffi("kv_incrby"));
        }
        Ok(out)
    }

    /// Set if not exists. Returns `true` if the key was set, `false` if it already existed.
    pub fn kv_setnx(&self, key: &[u8], value: &[u8], ttl_secs: i64) -> Result<bool, Error> {
        let mut was_set: std::os::raw::c_int = 0;
        let rc = unsafe {
            ffi::talon_kv_setnx(
                self.handle,
                key.as_ptr(),
                key.len(),
                value.as_ptr(),
                value.len(),
                ttl_secs,
                &mut was_set,
            )
        };
        if rc != 0 {
            return Err(Error::Ffi("kv_setnx"));
        }
        Ok(was_set != 0)
    }

    /// Insert a vector into the given index.
    pub fn vector_insert(&self, index_name: &str, id: u64, vector: &[f32]) -> Result<(), Error> {
        let c_name = CString::new(index_name)?;
        let rc = unsafe {
            ffi::talon_vector_insert(
                self.handle,
                c_name.as_ptr(),
                id,
                vector.as_ptr(),
                vector.len(),
            )
        };
        if rc != 0 {
            return Err(Error::Ffi("vector_insert"));
        }
        Ok(())
    }

    /// KNN vector search. Returns results as JSON string.
    pub fn vector_search(
        &self,
        index_name: &str,
        query: &[f32],
        k: usize,
        metric: &str,
    ) -> Result<String, Error> {
        let c_name = CString::new(index_name)?;
        let c_metric = CString::new(metric)?;
        let mut out: *mut std::os::raw::c_char = ptr::null_mut();
        let rc = unsafe {
            ffi::talon_vector_search(
                self.handle,
                c_name.as_ptr(),
                query.as_ptr(),
                query.len(),
                k,
                c_metric.as_ptr(),
                &mut out,
            )
        };
        if rc != 0 {
            return Err(Error::Ffi("vector_search"));
        }
        let result = unsafe { CStr::from_ptr(out).to_string_lossy().into_owned() };
        unsafe { ffi::talon_free_string(out) };
        Ok(result)
    }

    /// Flush all pending writes to disk.
    pub fn persist(&self) -> Result<(), Error> {
        let rc = unsafe { ffi::talon_persist(self.handle) };
        if rc != 0 {
            return Err(Error::Ffi("persist"));
        }
        Ok(())
    }

    /// Execute a generic JSON command covering all engine modules.
    ///
    /// Input format: `{"module":"kv|sql|ts|mq|vector|ai|backup|stats","action":"...","params":{...}}`
    /// Output format: `{"ok":true,"data":{...}}` or `{"ok":false,"error":"..."}`
    pub fn execute(&self, cmd_json: &str) -> Result<String, Error> {
        let c_cmd = CString::new(cmd_json)?;
        let mut out: *mut std::os::raw::c_char = ptr::null_mut();
        let rc = unsafe { ffi::talon_execute(self.handle, c_cmd.as_ptr(), &mut out) };
        if rc != 0 {
            return Err(Error::Ffi("execute"));
        }
        let result = unsafe { CStr::from_ptr(out).to_string_lossy().into_owned() };
        unsafe { ffi::talon_free_string(out) };
        Ok(result)
    }
}

impl Drop for Talon {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { ffi::talon_close(self.handle) };
            self.handle = ptr::null_mut();
        }
    }
}
