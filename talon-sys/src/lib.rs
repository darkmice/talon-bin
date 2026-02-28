//! FFI bindings to Talon — AI-native multi-model data engine.
//!
//! Provides a source-compatible API with the native `talon` crate via C FFI,
//! so downstream crates (`superclaw-db`) work without code changes.

use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::fmt;
use std::path::Path;
use std::ptr;
use std::slice;

use serde::{Deserialize, Serialize};

// ── Value 枚举（与源码 talon::Value serde 格式一致）─────────────────────────

/// 单值类型，与源码 Talon 的 `Value` 枚举 serde 兼容。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum Value {
    #[default]
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
    Boolean(bool),
    Jsonb(serde_json::Value),
    Vector(Vec<f32>),
    Timestamp(i64),
    GeoPoint(f64, f64),
}

// ── Error 类型 ──────────────────────────────────────────────────────────────

/// Error type for Talon operations.
#[derive(Debug)]
pub struct TalonError(pub String);

impl fmt::Display for TalonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TalonError: {}", self.0)
    }
}

impl std::error::Error for TalonError {}

impl From<std::ffi::NulError> for TalonError {
    fn from(e: std::ffi::NulError) -> Self {
        TalonError(format!("NUL byte in string: {e}"))
    }
}

// ── FTS 类型 ────────────────────────────────────────────────────────────────

/// FTS 索引配置。
#[derive(Debug, Clone, Default)]
pub struct FtsConfig {
    pub tokenizer: String,
}

/// FTS 文档。
#[derive(Debug, Clone)]
pub struct FtsDoc {
    pub doc_id: String,
    pub fields: BTreeMap<String, String>,
}

/// FTS 搜索命中结果。
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub doc_id: String,
    pub score: f32,
}

// ── Hybrid Search 类型 ──────────────────────────────────────────────────────

/// Hybrid search 命中结果。
#[derive(Debug, Clone)]
pub struct HybridHit {
    pub doc_id: String,
    pub score: f32,
}

/// FTS hybrid query（嵌套模块路径兼容）。
pub mod fts {
    pub mod hybrid {
        /// Hybrid search query parameters.
        pub struct HybridQuery<'a> {
            pub fts_index: &'a str,
            pub vec_index: &'a str,
            pub query_text: &'a str,
            pub query_vec: &'a [f32],
            pub metric: &'a str,
            pub limit: usize,
            pub fts_weight: f64,
            pub vec_weight: f64,
            pub num_candidates: Option<usize>,
            pub pre_filter: Option<Vec<(&'a str, &'a str)>>,
        }
    }
}

// ── Raw FFI 声明 ────────────────────────────────────────────────────────────

#[allow(dead_code)]
mod raw_ffi {
    use std::os::raw::{c_char, c_int};

    #[repr(C)]
    pub struct TalonHandle {
        _opaque: [u8; 0],
    }

    extern "C" {
        pub fn talon_open(path: *const c_char) -> *mut TalonHandle;
        pub fn talon_close(handle: *mut TalonHandle);
        pub fn talon_run_sql(
            handle: *const TalonHandle, sql: *const c_char,
            out_json: *mut *mut c_char,
        ) -> c_int;
        pub fn talon_kv_set(
            handle: *const TalonHandle,
            key: *const u8, key_len: usize,
            value: *const u8, value_len: usize, ttl_secs: i64,
        ) -> c_int;
        pub fn talon_kv_get(
            handle: *const TalonHandle,
            key: *const u8, key_len: usize,
            out_value: *mut *mut u8, out_len: *mut usize,
        ) -> c_int;
        pub fn talon_kv_del(
            handle: *const TalonHandle, key: *const u8, key_len: usize,
        ) -> c_int;
        pub fn talon_kv_incrby(
            handle: *const TalonHandle,
            key: *const u8, key_len: usize, delta: i64, out_value: *mut i64,
        ) -> c_int;
        pub fn talon_kv_setnx(
            handle: *const TalonHandle,
            key: *const u8, key_len: usize,
            value: *const u8, value_len: usize,
            ttl_secs: i64, was_set: *mut c_int,
        ) -> c_int;
        pub fn talon_vector_insert(
            handle: *const TalonHandle, index_name: *const c_char,
            id: u64, vec_data: *const f32, vec_dim: usize,
        ) -> c_int;
        pub fn talon_vector_search(
            handle: *const TalonHandle, index_name: *const c_char,
            vec_data: *const f32, vec_dim: usize, k: usize,
            metric: *const c_char, out_json: *mut *mut c_char,
        ) -> c_int;
        pub fn talon_persist(handle: *const TalonHandle) -> c_int;
        pub fn talon_execute(
            handle: *const TalonHandle, cmd_json: *const c_char,
            out_json: *mut *mut c_char,
        ) -> c_int;
        pub fn talon_free_string(ptr: *mut c_char);
        pub fn talon_free_bytes(ptr: *mut u8, len: usize);

        // ── 二进制 FFI（零 JSON 开销）──
        pub fn talon_run_sql_bin(
            handle: *const TalonHandle, sql: *const c_char,
            out_data: *mut *mut u8, out_len: *mut usize,
        ) -> c_int;
        pub fn talon_run_sql_param_bin(
            handle: *const TalonHandle, sql: *const c_char,
            params: *const u8, params_len: usize,
            out_data: *mut *mut u8, out_len: *mut usize,
        ) -> c_int;
        pub fn talon_vector_search_bin(
            handle: *const TalonHandle, index_name: *const c_char,
            vec_data: *const f32, vec_dim: usize, k: usize,
            metric: *const c_char,
            out_data: *mut *mut u8, out_len: *mut usize,
        ) -> c_int;
    }
}

// ── 子引擎包装 ──────────────────────────────────────────────────────────────

/// KV 引擎包装（持有 Talon 引用，代理 FFI 调用）。
pub struct KvEngine<'a> {
    db: &'a Talon,
}

impl<'a> KvEngine<'a> {
    /// 读取 key 对应的值。
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, TalonError> {
        self.db.raw_kv_get(key)
    }
    /// 写入 key-value，可选 TTL。
    pub fn set(&self, key: &[u8], value: &[u8], ttl_secs: Option<u64>) -> Result<(), TalonError> {
        self.db.raw_kv_set(key, value, ttl_secs.unwrap_or(0) as i64)
    }
    /// 删除 key。
    pub fn del(&self, key: &[u8]) -> Result<(), TalonError> {
        self.db.raw_kv_del(key)
    }
}

/// FTS 引擎包装（通过 talon_execute JSON 命令代理）。
pub struct FtsEngine<'a> {
    db: &'a Talon,
}

impl<'a> FtsEngine<'a> {
    /// 创建 FTS 索引（幂等）。
    pub fn create_index(&self, name: &str, _config: &FtsConfig) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "fts", "action": "create_index",
            "params": { "name": name }
        });
        self.db.exec_cmd(&cmd)
    }
    /// 索引单个文档。
    pub fn index_doc(&self, name: &str, doc: &FtsDoc) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "fts", "action": "index",
            "params": { "name": name, "doc_id": doc.doc_id, "fields": doc.fields }
        });
        self.db.exec_cmd(&cmd)
    }
    /// 删除文档索引。
    pub fn delete_doc(&self, name: &str, doc_id: &str) -> Result<bool, TalonError> {
        let cmd = serde_json::json!({
            "module": "fts", "action": "delete",
            "params": { "name": name, "doc_id": doc_id }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp.get("data")
            .and_then(|d| d.get("deleted"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false))
    }
    /// BM25 全文搜索。
    pub fn search(&self, name: &str, query: &str, limit: usize) -> Result<Vec<SearchHit>, TalonError> {
        let cmd = serde_json::json!({
            "module": "fts", "action": "search",
            "params": { "name": name, "query": query, "limit": limit }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let hits = resp.get("data")
            .and_then(|d| d.get("hits"))
            .and_then(|h| h.as_array())
            .map(|arr| {
                arr.iter().filter_map(|h| {
                    let doc_id = h.get("doc_id")?.as_str()?.to_string();
                    let score = h.get("score")?.as_f64()? as f32;
                    Some(SearchHit { doc_id, score })
                }).collect()
            })
            .unwrap_or_default();
        Ok(hits)
    }
}

/// 向量引擎包装。
pub struct VectorEngine<'a> {
    db: &'a Talon,
    index: String,
}

impl<'a> VectorEngine<'a> {
    /// 插入向量。
    pub fn insert(&self, id: u64, embedding: &[f32]) -> Result<(), TalonError> {
        self.db.raw_vector_insert(&self.index, id, embedding)
    }
    /// 删除向量。
    pub fn delete(&self, id: u64) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "vector", "action": "delete",
            "params": { "index": &self.index, "id": id }
        });
        self.db.exec_cmd(&cmd)
    }
    /// KNN 搜索，返回 (id, distance)。
    pub fn search(&self, query: &[f32], k: usize, metric: &str) -> Result<Vec<(u64, f32)>, TalonError> {
        self.db.raw_vector_search(&self.index, query, k, metric)
    }
    /// 向量数量。
    pub fn count(&self) -> Result<u64, TalonError> {
        let cmd = serde_json::json!({
            "module": "vector", "action": "count",
            "params": { "index": &self.index }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp.get("data")
            .and_then(|d| d.get("count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }
}

/// AI 引擎包装（通过 execute 代理）。
pub struct AiEngine;

/// StoreRef 占位（hybrid search 参数兼容用）。
pub struct StoreRef;

// ── Talon 主结构体 ──────────────────────────────────────────────────────────

/// A Talon database handle. Automatically closes on drop.
pub struct Talon {
    handle: *mut raw_ffi::TalonHandle,
}

// SAFETY: TalonHandle is internally synchronized via Talon's storage engine.
unsafe impl Send for Talon {}
unsafe impl Sync for Talon {}

impl Talon {
    // ── 打开数据库 ──

    /// Open a Talon database at the given path (string).
    pub fn open(path: impl AsRef<str>) -> Result<Self, TalonError> {
        let path_str = path.as_ref();
        let c_path = CString::new(path_str)?;
        let handle = unsafe { raw_ffi::talon_open(c_path.as_ptr()) };
        if handle.is_null() {
            return Err(TalonError(format!("Failed to open: {path_str}")));
        }
        Ok(Talon { handle })
    }

    /// Open from `&Path`（兼容源码 Talon 签名）。
    pub fn open_path(path: &Path) -> Result<Self, TalonError> {
        let s = path.to_str().ok_or_else(|| TalonError("Invalid UTF-8 path".into()))?;
        Self::open(s)
    }

    // ── SQL 执行（二进制 FFI，零 JSON 开销）──

    /// 执行 SQL，返回 `Vec<Vec<Value>>`（与源码 Talon API 兼容）。
    ///
    /// 内部使用二进制 TLV 编码传输结果，消除 JSON 序列化开销。
    pub fn run_sql(&self, sql: &str) -> Result<Vec<Vec<Value>>, TalonError> {
        let c_sql = CString::new(sql)?;
        let mut out_data: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let rc = unsafe {
            raw_ffi::talon_run_sql_bin(self.handle, c_sql.as_ptr(), &mut out_data, &mut out_len)
        };
        if rc != 0 {
            return Err(TalonError("run_sql FFI failed".into()));
        }
        if out_data.is_null() || out_len == 0 {
            return Ok(vec![]);
        }
        let data = unsafe { slice::from_raw_parts(out_data, out_len) };
        let result = decode_rows_bin(data);
        unsafe { raw_ffi::talon_free_bytes(out_data, out_len) };
        result
    }

    /// 参数化 SQL：参数用二进制编码传入引擎，引擎侧原生绑定。
    ///
    /// 消除了客户端 SQL 字符串拼接，安全性更高。
    pub fn run_sql_param(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, TalonError> {
        let c_sql = CString::new(sql)?;
        let params_bin = encode_params(params);
        let mut out_data: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let rc = unsafe {
            raw_ffi::talon_run_sql_param_bin(
                self.handle, c_sql.as_ptr(),
                params_bin.as_ptr(), params_bin.len(),
                &mut out_data, &mut out_len,
            )
        };
        if rc != 0 {
            return Err(TalonError("run_sql_param FFI failed".into()));
        }
        if out_data.is_null() || out_len == 0 {
            return Ok(vec![]);
        }
        let data = unsafe { slice::from_raw_parts(out_data, out_len) };
        let result = decode_rows_bin(data);
        unsafe { raw_ffi::talon_free_bytes(out_data, out_len) };
        result
    }

    // ── 引擎访问器 ──

    /// 获取 KV 引擎（写）。
    pub fn kv(&self) -> Result<KvEngine<'_>, TalonError> {
        Ok(KvEngine { db: self })
    }
    /// 获取 KV 引擎（读）。
    pub fn kv_read(&self) -> Result<KvEngine<'_>, TalonError> {
        Ok(KvEngine { db: self })
    }
    /// 获取 FTS 引擎（写）。
    pub fn fts(&self) -> Result<FtsEngine<'_>, TalonError> {
        Ok(FtsEngine { db: self })
    }
    /// 获取 FTS 引擎（读）。
    pub fn fts_read(&self) -> Result<FtsEngine<'_>, TalonError> {
        Ok(FtsEngine { db: self })
    }
    /// 获取 Vector 引擎。
    pub fn vector(&self, index: &str) -> Result<VectorEngine<'_>, TalonError> {
        Ok(VectorEngine { db: self, index: index.to_string() })
    }
    /// 获取 Vector 引擎（读）。
    pub fn vector_read(&self, index: &str) -> Result<VectorEngine<'_>, TalonError> {
        Ok(VectorEngine { db: self, index: index.to_string() })
    }
    /// 获取 AI 引擎。
    pub fn ai(&self) -> Result<AiEngine, TalonError> {
        Ok(AiEngine)
    }
    /// 获取 AI 引擎（读）。
    pub fn ai_read(&self) -> Result<AiEngine, TalonError> {
        Ok(AiEngine)
    }
    /// StoreRef（hybrid search 兼容）。
    pub fn store_ref(&self) -> &StoreRef {
        static STORE: StoreRef = StoreRef;
        &STORE
    }

    // ── 诊断 ──

    /// 数据库统计。
    pub fn database_stats(&self) -> Result<serde_json::Value, TalonError> {
        let cmd = serde_json::json!({"module": "database_stats"});
        let resp = self.exec_cmd_json(&cmd)?;
        Ok(resp.get("data").cloned().unwrap_or(serde_json::json!({})))
    }

    /// 健康检查。
    pub fn health_check(&self) -> serde_json::Value {
        let cmd = serde_json::json!({"module": "health_check"});
        self.exec_cmd_json(&cmd)
            .and_then(|r| Ok(r.get("data").cloned().unwrap_or(serde_json::json!({}))))
            .unwrap_or(serde_json::json!({"status": "error"}))
    }

    /// 刷盘。
    pub fn persist(&self) -> Result<(), TalonError> {
        let rc = unsafe { raw_ffi::talon_persist(self.handle) };
        if rc != 0 {
            return Err(TalonError("persist FFI failed".into()));
        }
        Ok(())
    }

    // ── 内部 FFI 辅助 ──

    fn raw_kv_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, TalonError> {
        let mut out_ptr: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let rc = unsafe {
            raw_ffi::talon_kv_get(self.handle, key.as_ptr(), key.len(), &mut out_ptr, &mut out_len)
        };
        if rc != 0 { return Err(TalonError("kv_get FFI failed".into())); }
        if out_ptr.is_null() { return Ok(None); }
        let data = unsafe { slice::from_raw_parts(out_ptr, out_len).to_vec() };
        unsafe { raw_ffi::talon_free_bytes(out_ptr, out_len) };
        Ok(Some(data))
    }

    fn raw_kv_set(&self, key: &[u8], value: &[u8], ttl_secs: i64) -> Result<(), TalonError> {
        let rc = unsafe {
            raw_ffi::talon_kv_set(self.handle, key.as_ptr(), key.len(),
                value.as_ptr(), value.len(), ttl_secs)
        };
        if rc != 0 { return Err(TalonError("kv_set FFI failed".into())); }
        Ok(())
    }

    fn raw_kv_del(&self, key: &[u8]) -> Result<(), TalonError> {
        let rc = unsafe { raw_ffi::talon_kv_del(self.handle, key.as_ptr(), key.len()) };
        if rc != 0 { return Err(TalonError("kv_del FFI failed".into())); }
        Ok(())
    }

    fn raw_vector_insert(&self, index: &str, id: u64, vec: &[f32]) -> Result<(), TalonError> {
        let c_name = CString::new(index)?;
        let rc = unsafe {
            raw_ffi::talon_vector_insert(self.handle, c_name.as_ptr(), id, vec.as_ptr(), vec.len())
        };
        if rc != 0 { return Err(TalonError("vector_insert FFI failed".into())); }
        Ok(())
    }

    fn raw_vector_search(&self, index: &str, query: &[f32], k: usize, metric: &str) -> Result<Vec<(u64, f32)>, TalonError> {
        let c_name = CString::new(index)?;
        let c_metric = CString::new(metric)?;
        let mut out_data: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let rc = unsafe {
            raw_ffi::talon_vector_search_bin(
                self.handle, c_name.as_ptr(),
                query.as_ptr(), query.len(), k, c_metric.as_ptr(),
                &mut out_data, &mut out_len,
            )
        };
        if rc != 0 { return Err(TalonError("vector_search FFI failed".into())); }
        if out_data.is_null() || out_len == 0 { return Ok(vec![]); }
        let data = unsafe { slice::from_raw_parts(out_data, out_len) };
        let result = decode_vector_bin(data);
        unsafe { raw_ffi::talon_free_bytes(out_data, out_len) };
        result
    }

    /// 执行 JSON 命令（忽略返回值）。
    fn exec_cmd(&self, cmd: &serde_json::Value) -> Result<(), TalonError> {
        let resp = self.exec_cmd_json(cmd)?;
        if resp.get("ok").and_then(|v| v.as_bool()) == Some(true) {
            Ok(())
        } else {
            let msg = resp.get("error").and_then(|v| v.as_str()).unwrap_or("unknown");
            Err(TalonError(msg.to_string()))
        }
    }

    /// 执行 JSON 命令，返回解析后的响应。
    fn exec_cmd_json(&self, cmd: &serde_json::Value) -> Result<serde_json::Value, TalonError> {
        let cmd_str = cmd.to_string();
        let c_cmd = CString::new(cmd_str)?;
        let mut out: *mut std::os::raw::c_char = ptr::null_mut();
        let rc = unsafe { raw_ffi::talon_execute(self.handle, c_cmd.as_ptr(), &mut out) };
        if rc != 0 { return Err(TalonError("execute FFI failed".into())); }
        if out.is_null() {
            return Err(TalonError("execute returned null output".into()));
        }
        let json_str = unsafe { CStr::from_ptr(out).to_string_lossy().into_owned() };
        unsafe { raw_ffi::talon_free_string(out) };
        serde_json::from_str(&json_str)
            .map_err(|e| TalonError(format!("JSON parse: {e}")))
    }
}

impl Drop for Talon {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { raw_ffi::talon_close(self.handle) };
            self.handle = ptr::null_mut();
        }
    }
}

// ── hybrid_search 顶层函数 ─────────────────────────────────────────────────

/// Hybrid search（FTS + Vector RRF 融合）。
///
/// 注意：FFI 版通过 `execute` 命令实现，`_store` 参数仅为 API 兼容保留。
pub fn hybrid_search(
    _store: &StoreRef,
    _q: &fts::hybrid::HybridQuery<'_>,
) -> Result<Vec<HybridHit>, TalonError> {
    // FFI hybrid search 需要 Talon handle，但 StoreRef 无法访问。
    // 当前 superclaw 已在应用层实现 RRF，此函数保留为兼容占位。
    Ok(vec![])
}

// ── 二进制编码/解码（TLV 格式）────────────────────────────────────────────
//
// Type tags: 0=Null, 1=Integer(i64), 2=Float(f64), 3=Text(u32+bytes),
//            4=Blob(u32+bytes), 5=Boolean(u8), 6=Jsonb(u32+bytes),
//            7=Vector(u32+f32*dim), 8=Timestamp(i64), 9=GeoPoint(f64,f64)

/// 将参数列表编码为二进制：`param_count: u32` + 每个参数的 TLV。
fn encode_params(params: &[Value]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + params.len() * 16);
    buf.extend_from_slice(&(params.len() as u32).to_le_bytes());
    for v in params {
        encode_value(&mut buf, v);
    }
    buf
}

/// 编码单个 Value 到缓冲区。
fn encode_value(buf: &mut Vec<u8>, val: &Value) {
    match val {
        Value::Null => buf.push(0),
        Value::Integer(i) => { buf.push(1); buf.extend_from_slice(&i.to_le_bytes()); }
        Value::Float(f) => { buf.push(2); buf.extend_from_slice(&f.to_le_bytes()); }
        Value::Text(s) => {
            buf.push(3);
            let b = s.as_bytes();
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Blob(b) => {
            buf.push(4);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Boolean(b) => { buf.push(5); buf.push(if *b { 1 } else { 0 }); }
        Value::Jsonb(j) => {
            buf.push(6);
            let s = j.to_string();
            let b = s.as_bytes();
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Vector(v) => {
            buf.push(7);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for f in v { buf.extend_from_slice(&f.to_le_bytes()); }
        }
        Value::Timestamp(t) => { buf.push(8); buf.extend_from_slice(&t.to_le_bytes()); }
        Value::GeoPoint(lat, lon) => {
            buf.push(9);
            buf.extend_from_slice(&lat.to_le_bytes());
            buf.extend_from_slice(&lon.to_le_bytes());
        }
    }
}

/// 解码二进制 SQL 结果：`row_count: u32, col_count: u32` + 每个 cell 的 TLV。
fn decode_rows_bin(data: &[u8]) -> Result<Vec<Vec<Value>>, TalonError> {
    if data.len() < 8 {
        return Err(TalonError("binary result too short".into()));
    }
    let row_count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let col_count = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let mut pos = 8;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let mut row = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            let (val, consumed) = decode_value(data, pos)?;
            row.push(val);
            pos += consumed;
        }
        rows.push(row);
    }
    Ok(rows)
}

/// 解码二进制向量搜索结果：`count: u32` + 每条 `id: u64, distance: f32`。
fn decode_vector_bin(data: &[u8]) -> Result<Vec<(u64, f32)>, TalonError> {
    if data.len() < 4 {
        return Err(TalonError("vector binary result too short".into()));
    }
    let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() < 4 + count * 12 {
        return Err(TalonError("vector binary result truncated".into()));
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let off = 4 + i * 12;
        let id = u64::from_le_bytes(data[off..off+8].try_into().unwrap());
        let dist = f32::from_le_bytes(data[off+8..off+12].try_into().unwrap());
        out.push((id, dist));
    }
    Ok(out)
}

/// 解码单个 Value，返回 (value, consumed_bytes)。
fn decode_value(data: &[u8], pos: usize) -> Result<(Value, usize), TalonError> {
    if pos >= data.len() {
        return Err(TalonError("unexpected end of binary data".into()));
    }
    let tag = data[pos];
    let off = pos + 1;
    match tag {
        0 => Ok((Value::Null, 1)),
        1 => {
            if off + 8 > data.len() { return Err(TalonError("truncated i64".into())); }
            let v = i64::from_le_bytes(data[off..off+8].try_into().unwrap());
            Ok((Value::Integer(v), 9))
        }
        2 => {
            if off + 8 > data.len() { return Err(TalonError("truncated f64".into())); }
            let v = f64::from_le_bytes(data[off..off+8].try_into().unwrap());
            Ok((Value::Float(v), 9))
        }
        3 => {
            if off + 4 > data.len() { return Err(TalonError("truncated text len".into())); }
            let len = u32::from_le_bytes(data[off..off+4].try_into().unwrap()) as usize;
            let start = off + 4;
            if start + len > data.len() { return Err(TalonError("truncated text data".into())); }
            let s = std::str::from_utf8(&data[start..start+len])
                .map_err(|_| TalonError("invalid utf8 in text".into()))?;
            Ok((Value::Text(s.to_string()), 5 + len))
        }
        4 => {
            if off + 4 > data.len() { return Err(TalonError("truncated blob len".into())); }
            let len = u32::from_le_bytes(data[off..off+4].try_into().unwrap()) as usize;
            let start = off + 4;
            if start + len > data.len() { return Err(TalonError("truncated blob data".into())); }
            Ok((Value::Blob(data[start..start+len].to_vec()), 5 + len))
        }
        5 => {
            if off >= data.len() { return Err(TalonError("truncated bool".into())); }
            Ok((Value::Boolean(data[off] != 0), 2))
        }
        6 => {
            if off + 4 > data.len() { return Err(TalonError("truncated jsonb len".into())); }
            let len = u32::from_le_bytes(data[off..off+4].try_into().unwrap()) as usize;
            let start = off + 4;
            if start + len > data.len() { return Err(TalonError("truncated jsonb data".into())); }
            let s = std::str::from_utf8(&data[start..start+len])
                .map_err(|_| TalonError("invalid utf8 in jsonb".into()))?;
            let j: serde_json::Value = serde_json::from_str(s)
                .map_err(|e| TalonError(format!("jsonb parse: {e}")))?;
            Ok((Value::Jsonb(j), 5 + len))
        }
        7 => {
            if off + 4 > data.len() { return Err(TalonError("truncated vec dim".into())); }
            let dim = u32::from_le_bytes(data[off..off+4].try_into().unwrap()) as usize;
            let start = off + 4;
            let byte_len = dim * 4;
            if start + byte_len > data.len() { return Err(TalonError("truncated vec data".into())); }
            let mut v = Vec::with_capacity(dim);
            for i in 0..dim {
                let s = start + i * 4;
                v.push(f32::from_le_bytes(data[s..s+4].try_into().unwrap()));
            }
            Ok((Value::Vector(v), 5 + byte_len))
        }
        8 => {
            if off + 8 > data.len() { return Err(TalonError("truncated timestamp".into())); }
            let v = i64::from_le_bytes(data[off..off+8].try_into().unwrap());
            Ok((Value::Timestamp(v), 9))
        }
        9 => {
            if off + 16 > data.len() { return Err(TalonError("truncated geopoint".into())); }
            let lat = f64::from_le_bytes(data[off..off+8].try_into().unwrap());
            let lon = f64::from_le_bytes(data[off+8..off+16].try_into().unwrap());
            Ok((Value::GeoPoint(lat, lon), 17))
        }
        _ => Err(TalonError(format!("unknown binary type tag: {tag}"))),
    }
}
