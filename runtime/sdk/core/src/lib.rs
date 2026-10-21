//! Core native handle/session adapter. Database implementation lives only in the binary.
mod value;
use serde_json::json;
use std::{
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use talon_runtime_transport::call_json;
pub use value::*;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Database busy: {0}")]
    Busy(String),
    #[error("SQL result indeterminate: {detail}")]
    SqlResultIndeterminate { detail: String },
    #[error("SQL durability: {detail}")]
    SqlDurability { detail: String },
    #[error("Serialization: {0}")]
    Serialization(String),
    #[error("SQL execution: {0}")]
    SqlExec(String),
}
impl From<talon_runtime_transport::Error> for Error {
    fn from(error: talon_runtime_transport::Error) -> Self {
        match error.code.as_str() {
            "busy" => Self::Busy(error.message),
            "result_indeterminate" => Self::SqlResultIndeterminate {
                detail: error.message,
            },
            "durability" => Self::SqlDurability {
                detail: error.message,
            },
            _ => Self::SqlExec(error.to_string()),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(u64);
impl SessionId {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}
impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}
struct Inner {
    id: u64,
}
impl Drop for Inner {
    fn drop(&mut self) {
        let _ = call_json(json!({"action":"db_close","db":self.id}));
    }
}
#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}
impl Store {
    pub fn native_handle(&self) -> u64 {
        self.inner.id
    }
}
#[derive(Clone)]
pub struct SharedTalon {
    store: Store,
}
pub struct Talon;
impl Talon {
    pub fn open_shared(path: impl AsRef<Path>) -> Result<SharedTalon, Error> {
        let result = call_json(json!({"action":"db_open","path":path.as_ref()}))?;
        let id = result["id"]
            .as_u64()
            .ok_or_else(|| Error::Serialization("Missing database handle".into()))?;
        Ok(SharedTalon {
            store: Store {
                inner: Arc::new(Inner { id }),
            },
        })
    }
    pub fn open(path: impl AsRef<Path>) -> Result<SharedTalon, Error> {
        Self::open_shared(path)
    }
}
impl SharedTalon {
    pub fn store_ref(&self) -> &Store {
        &self.store
    }
    pub fn run_sql(&self, sql: &str) -> Result<Vec<Vec<Value>>, Error> {
        self.run_sql_param(sql, &[])
    }
    pub fn run_sql_param(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, Error> {
        self.query(sql, params, None)
    }
    pub fn run_sql_param_as(
        &self,
        session: SessionId,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<Vec<Value>>, Error> {
        self.query(sql, params, Some(session.0))
    }
    fn query(
        &self,
        sql: &str,
        params: &[Value],
        session: Option<u64>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let rows = call_json(
            json!({"action":"db_query","db":self.store.inner.id,"sql":sql,"params":params,"session":session}),
        )?;
        serde_json::from_value(rows).map_err(|_| Error::Serialization("Invalid native rows".into()))
    }
    pub fn end_session(&self, session: SessionId) {
        let _ = call_json(
            json!({"action":"db_end_session","db":self.store.inner.id,"session":session.0}),
        );
    }
    pub fn persist(&self) -> Result<(), Error> {
        call_json(json!({"action":"db_persist","db":self.store.inner.id}))
            .map(|_| ())
            .map_err(Into::into)
    }
}
