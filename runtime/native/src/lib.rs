//! Native runtime v1: byte ABI, opaque owned handles, bounded async operations.
mod agent;
mod provider;
#[cfg(test)]
mod tests;
use futures::FutureExt;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    ffi::{c_char, CString},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
};
use tokio_util::sync::CancellationToken;

const MAX_MESSAGE: usize = 8 * 1024 * 1024;
const MAX_QUEUE_BYTES: usize = 16 * 1024 * 1024;
const MAX_OPERATIONS: usize = 32;
const MAX_DATABASES: usize = 64;
type Result<T> = std::result::Result<T, Failure>;
#[derive(Debug)]
struct Failure {
    code: &'static str,
    message: String,
}
fn failure(message: &str) -> Failure {
    Failure {
        code: "invalid_request",
        message: message.into(),
    }
}
impl From<serde_json::Error> for Failure {
    fn from(_: serde_json::Error) -> Self {
        failure("Invalid runtime wire value")
    }
}
impl From<talon::Error> for Failure {
    fn from(error: talon::Error) -> Self {
        let code = match &error {
            talon::Error::Busy(_) => "busy",
            talon::Error::SqlResultIndeterminate { .. } => "result_indeterminate",
            talon::Error::SqlDurability { .. } => "durability",
            _ => "sql_error",
        };
        Self {
            code,
            message: error.to_string(),
        }
    }
}
struct Database {
    core: talon::SharedTalon,
    sessions: Mutex<HashMap<u64, talon::SessionId>>,
}
impl Drop for Database {
    fn drop(&mut self) {
        for session in self
            .sessions
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            self.core.end_session(*session);
        }
    }
}
struct Reply {
    value: Value,
    done: bool,
}
struct Replies {
    receiver: tokio::sync::mpsc::Receiver<Reply>,
    operation: std::sync::Weak<Operation>,
    id: u64,
}
impl Drop for Replies {
    fn drop(&mut self) {
        if let Some(op) = self.operation.upgrade() {
            if op
                .callbacks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.id)
                .is_some()
            {
                let mut cancelled = op
                    .cancelled_callbacks
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if cancelled.len() < 128 {
                    cancelled.push_back(self.id);
                } else {
                    op.cancel.cancel();
                }
            }
        }
    }
}
struct Queue {
    messages: VecDeque<(Value, usize)>,
    bytes: usize,
}
struct Operation {
    cancel: CancellationToken,
    events: Mutex<Queue>,
    terminal: Mutex<Option<Value>>,
    space: tokio::sync::Notify,
    callbacks: Mutex<HashMap<u64, tokio::sync::mpsc::Sender<Reply>>>,
    next_callback: AtomicU64,
    cancelled_callbacks: Mutex<VecDeque<u64>>,
    hitl: Arc<talon_agent::HitlChannel>,
    hitl_pending: AtomicUsize,
    finished: Mutex<bool>,
    finished_signal: std::sync::Condvar,
}
struct State {
    databases: HashMap<u64, Arc<Database>>,
    operations: HashMap<u64, Arc<Operation>>,
    next: u64,
}
fn state() -> &'static Mutex<State> {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    STATE.get_or_init(|| {
        Mutex::new(State {
            databases: HashMap::new(),
            operations: HashMap::new(),
            next: 1,
        })
    })
}
static TASKS: AtomicUsize = AtomicUsize::new(0);
fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .thread_name("talon-native")
            .build()
            .expect("native runtime initialization")
    })
}
fn id(request: &Value, key: &str) -> Result<u64> {
    request[key]
        .as_u64()
        .filter(|v| *v > 0)
        .ok_or_else(|| failure("Missing opaque ID"))
}
fn text<'a>(request: &'a Value, key: &str) -> Result<&'a str> {
    request[key]
        .as_str()
        .ok_or_else(|| failure("Missing text field"))
}
fn database(request: &Value) -> Result<Arc<Database>> {
    let id = id(request, "db")?;
    state()
        .lock()
        .map_err(|_| failure("Runtime registry poisoned"))?
        .databases
        .get(&id)
        .cloned()
        .ok_or_else(|| failure("Database handle is closed"))
}
fn operation(request: &Value) -> Result<Arc<Operation>> {
    let id = id(request, "id")?;
    state()
        .lock()
        .map_err(|_| failure("Runtime registry poisoned"))?
        .operations
        .get(&id)
        .cloned()
        .ok_or_else(|| failure("Operation handle is closed"))
}
fn next_id(state: &mut State) -> Result<u64> {
    let id = state.next;
    state.next = state
        .next
        .checked_add(1)
        .ok_or_else(|| failure("Opaque ID space exhausted"))?;
    Ok(id)
}

impl Operation {
    async fn emit(&self, event: Value) -> Result<()> {
        let size = serde_json::to_vec(&event)?.len();
        if size > MAX_MESSAGE - 4096 {
            return Err(failure("Runtime event exceeds wire limit"));
        }
        loop {
            let notified = self.space.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut queue = self
                    .events
                    .lock()
                    .map_err(|_| failure("Event queue poisoned"))?;
                if queue.messages.len() < 128 && queue.bytes + size <= MAX_QUEUE_BYTES {
                    queue.bytes += size;
                    queue.messages.push_back((event, size));
                    return Ok(());
                }
            }
            tokio::select! {_=self.cancel.cancelled()=>return Err(failure("Operation cancelled under backpressure")),_=notified=>{}}
        }
    }
    async fn callback(self: &Arc<Self>, mut event: Value) -> Result<(u64, Replies)> {
        let id = self.next_callback.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            return Err(failure("Callback IDs exhausted"));
        }
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        {
            let mut callbacks = self
                .callbacks
                .lock()
                .map_err(|_| failure("Callback registry poisoned"))?;
            if callbacks.len() >= 64 {
                return Err(failure("Callback capacity exceeded"));
            }
            callbacks.insert(id, tx);
        }
        event["kind"] = json!("callback");
        event["callbackId"] = json!(id);
        if let Err(error) = self.emit(event).await {
            self.callbacks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id);
            return Err(error);
        }
        Ok((
            id,
            Replies {
                receiver: rx,
                operation: Arc::downgrade(self),
                id,
            },
        ))
    }
    async fn receive(&self, rx: &mut Replies) -> Result<Reply> {
        tokio::select! {biased;_=self.cancel.cancelled()=>Err(failure("Operation cancelled")),value=rx.receiver.recv()=>value.ok_or_else(||failure("Host callback closed"))}
    }
}

fn dispatch(request: Value) -> Result<Value> {
    match text(&request, "action")? {
        "identity" => {
            let mut pointer = std::ptr::null_mut();
            let code = unsafe { talon::ffi::talon_build_manifest(&mut pointer) };
            if code != 0 || pointer.is_null() {
                return Err(failure("Core identity unavailable"));
            }
            let result = unsafe {
                let value =
                    serde_json::from_slice::<Value>(std::ffi::CStr::from_ptr(pointer).to_bytes());
                talon::ffi::talon_free_string(pointer);
                value?
            };
            let state = state()
                .lock()
                .map_err(|_| failure("Runtime registry poisoned"))?;
            Ok(
                json!({"protocolVersion":1,"profile":"platform-host-v1","runtimeVersion":env!("CARGO_PKG_VERSION"),"producerCommit":env!("TALON_RUNTIME_PRODUCER_COMMIT"),"producerDirty":env!("TALON_RUNTIME_PRODUCER_DIRTY")=="true","sources":serde_json::from_str::<Value>(include_str!("../sources.lock.json"))?,"core":result,"activeDatabases":state.databases.len(),"activeOperations":state.operations.len(),"activeTasks":TASKS.load(Ordering::Acquire)}),
            )
        }
        "db_open" => {
            // Open before acquiring the registry lock. Core itself serializes physical ownership.
            let core = talon::Talon::open_shared(text(&request, "path")?)?;
            let mut state = state()
                .lock()
                .map_err(|_| failure("Runtime registry poisoned"))?;
            if state.databases.len() >= MAX_DATABASES {
                return Err(failure("Database handle capacity exceeded"));
            }
            let id = next_id(&mut state)?;
            state.databases.insert(
                id,
                Arc::new(Database {
                    core,
                    sessions: Mutex::new(HashMap::new()),
                }),
            );
            Ok(json!({"id":id}))
        }
        "db_close" => {
            let id = id(&request, "db")?;
            let db = state()
                .lock()
                .map_err(|_| failure("Runtime registry poisoned"))?
                .databases
                .remove(&id)
                .ok_or_else(|| failure("Database handle is closed"))?;
            drop(db);
            Ok(Value::Null)
        }
        "db_query" => {
            let db = database(&request)?;
            let sql = text(&request, "sql")?;
            let params: Vec<talon::Value> = serde_json::from_value(request["params"].clone())?;
            let rows = if let Some(session) = request["session"].as_u64() {
                if session == 0 {
                    return Err(failure("Invalid SQL session"));
                }
                let session = {
                    let mut sessions = db
                        .sessions
                        .lock()
                        .map_err(|_| failure("Session registry poisoned"))?;
                    if !sessions.contains_key(&session) && sessions.len() >= 1024 {
                        return Err(failure("SQL session capacity exceeded"));
                    }
                    *sessions
                        .entry(session)
                        .or_insert_with(talon::SessionId::new)
                };
                db.core.run_sql_param_as(session, sql, &params)?
            } else {
                db.core.run_sql_param(sql, &params)?
            };
            Ok(serde_json::to_value(rows)?)
        }
        "db_end_session" => {
            let db = database(&request)?;
            let session = db
                .sessions
                .lock()
                .map_err(|_| failure("Session registry poisoned"))?
                .remove(&id(&request, "session")?);
            if let Some(session) = session {
                db.core.end_session(session);
            }
            Ok(Value::Null)
        }
        "db_persist" => {
            database(&request)?.core.persist()?;
            Ok(Value::Null)
        }
        "tokenize" => {
            let count = talon_llm::provider::openai_compat::OpenAICompatibleProvider::count_tokens(
                text(&request, "model")?,
                text(&request, "text")?,
            )
            .map_err(|_| failure("Native tokenizer failed"))?;
            Ok(json!(count))
        }
        "start" => {
            // Validate operation-specific inputs before allocating a handle/task.
            let job = match text(&request, "operation")? {
                "agent" => agent::prepare(&request)?,
                "provider" => provider::prepare(&request)?,
                "builtin" => agent::prepare_builtin(&request)?,
                _ => return Err(failure("Unsupported native operation")),
            };
            let executor = runtime();
            let op = Arc::new(Operation {
                cancel: CancellationToken::new(),
                events: Mutex::new(Queue {
                    messages: VecDeque::new(),
                    bytes: 0,
                }),
                terminal: Mutex::new(None),
                space: tokio::sync::Notify::new(),
                callbacks: Mutex::new(HashMap::new()),
                cancelled_callbacks: Mutex::new(VecDeque::new()),
                next_callback: AtomicU64::new(1),
                hitl: Arc::new(talon_agent::HitlChannel::new()),
                hitl_pending: AtomicUsize::new(0),
                finished: Mutex::new(false),
                finished_signal: std::sync::Condvar::new(),
            });
            let id = {
                let mut state = state()
                    .lock()
                    .map_err(|_| failure("Runtime registry poisoned"))?;
                if state.operations.len() >= MAX_OPERATIONS {
                    return Err(failure("Operation capacity exceeded"));
                }
                let id = next_id(&mut state)?;
                state.operations.insert(id, op.clone());
                id
            };
            TASKS.fetch_add(1, Ordering::AcqRel);
            executor.spawn(async move{
    struct TaskOwner(Arc<Operation>);
    impl Drop for TaskOwner{fn drop(&mut self){TASKS.fetch_sub(1,Ordering::AcqRel);*self.0.finished.lock().unwrap_or_else(|e|e.into_inner())=true;self.0.finished_signal.notify_all();}}
    let _owner=TaskOwner(op.clone());
    let result=std::panic::AssertUnwindSafe(job(op.clone())).catch_unwind().await;
    let terminal=match result{Ok(Ok(value))=>json!({"kind":"done","result":value}),Ok(Err(error))=>json!({"kind":"error","code":error.code,"message":error.message}),Err(_)=>json!({"kind":"error","code":"native_panic","message":"Native operation panicked"})};
    *op.terminal.lock().unwrap_or_else(|e|e.into_inner())=Some(terminal);
    op.callbacks.lock().unwrap_or_else(|e|e.into_inner()).clear();
   });
            Ok(json!({"id":id}))
        }
        "poll" => {
            let op = operation(&request)?;
            let mut queue = op
                .events
                .lock()
                .map_err(|_| failure("Event queue poisoned"))?;
            let mut messages = vec![];
            let mut bytes = 0;
            {
                let mut cancelled = op
                    .cancelled_callbacks
                    .lock()
                    .map_err(|_| failure("Callback cancellation queue poisoned"))?;
                while messages.len() < 16 {
                    let Some(id) = cancelled.pop_front() else {
                        break;
                    };
                    messages.push(json!({"kind":"callback_cancel","callbackId":id}));
                }
            }
            while let Some((_, size)) = queue.messages.front() {
                if messages.len() >= 16 || bytes + size > MAX_MESSAGE - 2048 {
                    break;
                }
                let (value, size) = queue.messages.pop_front().unwrap();
                queue.bytes -= size;
                bytes += size;
                messages.push(value);
            }
            if queue.messages.is_empty() && bytes < MAX_MESSAGE / 2 {
                if let Some(terminal) = op
                    .terminal
                    .lock()
                    .map_err(|_| failure("Terminal state poisoned"))?
                    .take()
                {
                    messages.push(terminal);
                }
            }
            drop(queue);
            op.space.notify_waiters();
            Ok(json!(messages))
        }
        "reply" => {
            let op = operation(&request)?;
            let callback = id(&request, "callbackId")?;
            let done = request["done"]
                .as_bool()
                .ok_or_else(|| failure("Missing callback completion flag"))?;
            let mut callbacks = op
                .callbacks
                .lock()
                .map_err(|_| failure("Callback registry poisoned"))?;
            let sender = callbacks.get(&callback).ok_or_else(|| Failure {
                code: "callback_closed",
                message: "Stale or duplicate callback response".into(),
            })?;
            sender
                .try_send(Reply {
                    value: request["value"].clone(),
                    done,
                })
                .map_err(|error| match error {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => Failure {
                        code: "callback_busy",
                        message: "Callback queue is full".into(),
                    },
                    _ => failure("Callback receiver is closed"),
                })?;
            if done {
                callbacks.remove(&callback);
            }
            Ok(Value::Null)
        }
        "hitl" => {
            let op = operation(&request)?;
            let command: sdk_agent::HitlCommand =
                serde_json::from_value(request["command"].clone())?;
            agent::inject(&op, command)?;
            Ok(Value::Null)
        }
        "cancel" => {
            operation(&request)?.cancel.cancel();
            Ok(Value::Null)
        }
        "close" => {
            let id = id(&request, "id")?;
            let op = state()
                .lock()
                .map_err(|_| failure("Runtime registry poisoned"))?
                .operations
                .remove(&id)
                .ok_or_else(|| failure("Operation is closed"))?;
            op.cancel.cancel();
            op.space.notify_waiters();
            op.callbacks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clear();
            // No direct host callbacks run on the native executor. Cancellation
            // releases the task independently; wait for its DB ownership cleanup.
            let mut finished = op
                .finished
                .lock()
                .map_err(|_| failure("Operation completion poisoned"))?;
            while !*finished {
                finished = op
                    .finished_signal
                    .wait(finished)
                    .map_err(|_| failure("Operation completion poisoned"))?;
            }
            Ok(Value::Null)
        }
        _ => Err(failure("Unsupported runtime action")),
    }
}

type Job = Box<
    dyn FnOnce(
            Arc<Operation>,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send>>
        + Send,
>;

/// Caller owns valid input/output buffers. Input is borrowed for this call only;
/// output is allocated by this library and must use talon_runtime_free_v1 once.
#[no_mangle]
pub unsafe extern "C" fn talon_runtime_call_v1(
    input: *const u8,
    len: usize,
    output: *mut *mut c_char,
) -> i32 {
    if output.is_null() {
        return -1;
    }
    *output = std::ptr::null_mut();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if input.is_null() || len == 0 || len > MAX_MESSAGE {
            return Err(failure("Invalid input buffer length"));
        }
        let request = serde_json::from_slice(std::slice::from_raw_parts(input, len))?;
        dispatch(request)
    }));
    let envelope = match result {
        Ok(Ok(data)) => json!({"ok":true,"data":data}),
        Ok(Err(error)) => json!({"ok":false,"code":error.code,"message":error.message}),
        Err(_) => json!({"ok":false,"code":"native_panic","message":"Native call panicked"}),
    };
    let bytes=match serde_json::to_vec(&envelope){Ok(bytes) if bytes.len()<=MAX_MESSAGE=>bytes,_=>br#"{"ok":false,"code":"response_limit","message":"Native response exceeds wire limit"}"#.to_vec()};
    match CString::new(bytes) {
        Ok(string) => {
            *output = string.into_raw();
            0
        }
        Err(_) => -1,
    }
}
#[no_mangle]
pub unsafe extern "C" fn talon_runtime_free_v1(pointer: *mut c_char) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if !pointer.is_null() {
            drop(CString::from_raw(pointer));
        }
    }));
}
