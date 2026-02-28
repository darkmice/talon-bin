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

    // ── SQL 执行 ──

    /// 执行 SQL，返回 `Vec<Vec<Value>>`（与源码 Talon API 兼容）。
    pub fn run_sql(&self, sql: &str) -> Result<Vec<Vec<Value>>, TalonError> {
        let json_str = self.run_sql_raw(sql)?;
        parse_sql_rows(&json_str)
    }

    /// 参数化 SQL：安全替换 `?` 占位符后执行。
    pub fn run_sql_param(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, TalonError> {
        let bound = bind_params(sql, params);
        self.run_sql(&bound)
    }

    /// 原始 SQL 执行，返回 JSON 字符串。
    fn run_sql_raw(&self, sql: &str) -> Result<String, TalonError> {
        let c_sql = CString::new(sql)?;
        let mut out: *mut std::os::raw::c_char = ptr::null_mut();
        let rc = unsafe { raw_ffi::talon_run_sql(self.handle, c_sql.as_ptr(), &mut out) };
        if rc != 0 {
            return Err(TalonError("run_sql FFI failed".into()));
        }
        let result = unsafe { CStr::from_ptr(out).to_string_lossy().into_owned() };
        unsafe { raw_ffi::talon_free_string(out) };
        Ok(result)
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
        let mut out: *mut std::os::raw::c_char = ptr::null_mut();
        let rc = unsafe {
            raw_ffi::talon_vector_search(self.handle, c_name.as_ptr(),
                query.as_ptr(), query.len(), k, c_metric.as_ptr(), &mut out)
        };
        if rc != 0 { return Err(TalonError("vector_search FFI failed".into())); }
        let json_str = unsafe { CStr::from_ptr(out).to_string_lossy().into_owned() };
        unsafe { raw_ffi::talon_free_string(out) };
        parse_vector_results(&json_str)
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

// ── JSON 解析辅助 ───────────────────────────────────────────────────────────

/// 解析 `talon_run_sql` 返回的 JSON `{"rows": [[Value, ...], ...]}` 为 `Vec<Vec<Value>>`。
fn parse_sql_rows(json: &str) -> Result<Vec<Vec<Value>>, TalonError> {
    let parsed: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| TalonError(format!("SQL result JSON parse: {e}")))?;
    let rows = parsed.get("rows")
        .and_then(|r| r.as_array())
        .ok_or_else(|| TalonError("SQL result missing 'rows'".into()))?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let cols = row.as_array()
            .ok_or_else(|| TalonError("Row is not array".into()))?;
        let mut values = Vec::with_capacity(cols.len());
        for col in cols {
            let v: Value = serde_json::from_value(col.clone())
                .unwrap_or(Value::Null);
            values.push(v);
        }
        result.push(values);
    }
    Ok(result)
}

/// 解析向量搜索 JSON `{"results": [{"id": u64, "distance": f32}, ...]}`.
fn parse_vector_results(json: &str) -> Result<Vec<(u64, f32)>, TalonError> {
    let parsed: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| TalonError(format!("Vector result JSON parse: {e}")))?;
    let items = parsed.get("results")
        .and_then(|r| r.as_array())
        .ok_or_else(|| TalonError("Vector result missing 'results'".into()))?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let id = item.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
        let dist = item.get("distance").and_then(|v| v.as_f64()).unwrap_or(0.0) as f32;
        out.push((id, dist));
    }
    Ok(out)
}

// ── 参数替换 ────────────────────────────────────────────────────────────────

/// 安全的 SQL 参数替换：将 `?` 占位符替换为 Value 的 SQL 字面量。
///
/// 正确处理 SQL 字符串字面量（含转义引号 `''`，如 `'it''s a test'`），
/// 确保字面量内部的 `?` 不被当作参数占位符。
fn bind_params(sql: &str, params: &[Value]) -> String {
    let mut result = String::with_capacity(sql.len() + params.len() * 16);
    let mut idx = 0;
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '?' && idx < params.len() {
            result.push_str(&value_to_sql(&params[idx]));
            idx += 1;
        } else if ch == '\'' {
            // 跳过 SQL 字符串字面量内的所有内容（含 '' 转义）
            result.push(ch);
            loop {
                match chars.next() {
                    Some('\'') => {
                        result.push('\'');
                        // '' 是转义引号，继续在字符串内
                        if chars.peek() == Some(&'\'') {
                            result.push(chars.next().unwrap());
                        } else {
                            break; // 单个 ' = 字符串结束
                        }
                    }
                    Some(c) => result.push(c),
                    None => break, // 未闭合字符串（容错）
                }
            }
        } else {
            result.push(ch);
        }
    }
    result
}

/// Value → SQL 字面量（防注入）。
fn value_to_sql(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_nan() || f.is_infinite() { "NULL".to_string() }
            else { format!("{f}") }
        }
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
        Value::Timestamp(t) => t.to_string(),
        Value::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            format!("X'{hex}'")
        }
        Value::Jsonb(j) => format!("'{}'", j.to_string().replace('\'', "''")),
        Value::Vector(v) => {
            let inner: Vec<String> = v.iter().map(|f| f.to_string()).collect();
            format!("'[{}]'", inner.join(","))
        }
        Value::GeoPoint(lat, lon) => format!("POINT({lat},{lon})"),
    }
}
