/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FFI bindings to Talon — AI-native multi-model data engine.
//!
//! Provides a source-compatible API with the native `talon` crate via C FFI,
//! so downstream crates (`superclaw-db`) work without code changes.

use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::fmt;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::ptr;
use std::slice;
use std::sync::Mutex;
use std::time::Duration;

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

// ── Remote Client 类型 ─────────────────────────────────────────────────────

const DEFAULT_REMOTE_TIMEOUT_SECS: u64 = 5;
const MAX_REMOTE_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// Remote client error classes used by the `talon://` TCP client.
///
/// Remote APIs still return `TalonError` for source compatibility. Error
/// messages are prefixed with `remote <kind>:` using these stable classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TalonRemoteErrorKind {
    InvalidEndpoint,
    Connect,
    Timeout,
    Auth,
    Handshake,
    Protocol,
    Server,
    Io,
}

impl TalonRemoteErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TalonRemoteErrorKind::InvalidEndpoint => "invalid-endpoint",
            TalonRemoteErrorKind::Connect => "connect",
            TalonRemoteErrorKind::Timeout => "timeout",
            TalonRemoteErrorKind::Auth => "auth",
            TalonRemoteErrorKind::Handshake => "handshake",
            TalonRemoteErrorKind::Protocol => "protocol",
            TalonRemoteErrorKind::Server => "server",
            TalonRemoteErrorKind::Io => "io",
        }
    }
}

fn remote_error(kind: TalonRemoteErrorKind, msg: impl Into<String>) -> TalonError {
    TalonError(format!("remote {}: {}", kind.as_str(), msg.into()))
}

fn remote_io_error(
    fallback_kind: TalonRemoteErrorKind,
    context: &str,
    err: std::io::Error,
) -> TalonError {
    let kind = match err.kind() {
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => {
            TalonRemoteErrorKind::Timeout
        }
        _ => fallback_kind,
    };
    remote_error(kind, format!("{context}: {err}"))
}

#[derive(Debug, Clone)]
struct RemoteEndpoint {
    endpoint: String,
    addr: String,
    auth_token: Option<String>,
    timeout: Duration,
}

/// TCP client for a remote Talon server exposed at `talon://host:port`.
///
/// Supported URI forms:
/// - `talon://host:port`
/// - `talon://host:port?auth_token=secret`
/// - `talon://host:port?timeout_ms=5000`
///
/// The wire protocol matches the server TCP frame protocol:
/// `[4-byte big-endian length][JSON command payload]`.
#[derive(Debug)]
pub struct TalonRemoteClient {
    endpoint: String,
    addr: String,
    auth_token: Option<String>,
    timeout: Duration,
    stream: Mutex<TcpStream>,
}

impl TalonRemoteClient {
    /// Connect to a remote Talon TCP endpoint using the default 5s timeout.
    pub fn connect(endpoint: &str) -> Result<Self, TalonError> {
        Self::connect_with_timeout(endpoint, Duration::from_secs(DEFAULT_REMOTE_TIMEOUT_SECS))
    }

    /// Connect to a remote Talon TCP endpoint with an explicit timeout.
    pub fn connect_with_timeout(endpoint: &str, timeout: Duration) -> Result<Self, TalonError> {
        let parsed = parse_talon_remote_endpoint(endpoint, timeout)?;
        let mut stream = connect_remote_stream(&parsed)?;
        if let Some(token) = parsed.auth_token.as_deref() {
            authenticate_remote_stream(&mut stream, token)?;
        }
        Ok(Self {
            endpoint: parsed.endpoint,
            addr: parsed.addr,
            auth_token: parsed.auth_token,
            timeout: parsed.timeout,
            stream: Mutex::new(stream),
        })
    }

    /// Original endpoint string supplied by the caller.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Resolved `host:port` authority used for TCP connections.
    pub fn address(&self) -> &str {
        &self.addr
    }

    /// Read/write/connect timeout used by this client.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Whether the client performed token authentication during handshake.
    pub fn has_auth_token(&self) -> bool {
        self.auth_token.is_some()
    }

    /// Execute SQL remotely and return rows using the same `Value` shape as embedded mode.
    pub fn run_sql(&self, sql: &str) -> Result<Vec<Vec<Value>>, TalonError> {
        let cmd = serde_json::json!({
            "module": "sql",
            "action": "",
            "params": { "sql": sql }
        });
        let resp = self.exec_cmd_json(&cmd)?;
        let data = remote_response_data(&resp)?.ok_or_else(|| {
            remote_error(
                TalonRemoteErrorKind::Protocol,
                format!("SQL response missing data: {resp}"),
            )
        })?;
        let rows = data.get("rows").and_then(|r| r.as_array()).ok_or_else(|| {
            remote_error(
                TalonRemoteErrorKind::Protocol,
                format!("SQL response missing rows array: {resp}"),
            )
        })?;
        rows.iter()
            .map(|row| {
                row.as_array()
                    .ok_or_else(|| {
                        remote_error(
                            TalonRemoteErrorKind::Protocol,
                            format!("SQL row is not an array: {row}"),
                        )
                    })?
                    .iter()
                    .map(talon_value_from_json)
                    .collect()
            })
            .collect()
    }

    /// Execute parameterized SQL remotely.
    ///
    /// The current server JSON command accepts SQL text only, so the client
    /// renders `?` placeholders into SQL literals before sending the command.
    /// This keeps the public client surface aligned with embedded
    /// `run_sql_param` until the server protocol grows native bind params.
    pub fn run_sql_param(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<Vec<Value>>, TalonError> {
        if params.is_empty() {
            return self.run_sql(sql);
        }
        let rendered = inline_sql_params(sql, params)?;
        self.run_sql(&rendered)
    }

    /// Get a remote KV client surface.
    pub fn kv(&self) -> Result<RemoteKvEngine<'_>, TalonError> {
        Ok(RemoteKvEngine { client: self })
    }

    /// Get a remote KV read client surface.
    pub fn kv_read(&self) -> Result<RemoteKvEngine<'_>, TalonError> {
        Ok(RemoteKvEngine { client: self })
    }

    /// Get a remote MQ client surface.
    pub fn mq(&self) -> Result<RemoteMqEngine<'_>, TalonError> {
        Ok(RemoteMqEngine { client: self })
    }

    /// Get a remote MQ read client surface.
    pub fn mq_read(&self) -> Result<RemoteMqEngine<'_>, TalonError> {
        Ok(RemoteMqEngine { client: self })
    }

    /// Execute a raw JSON command against the remote server.
    pub fn exec_cmd_json(&self, cmd: &serde_json::Value) -> Result<serde_json::Value, TalonError> {
        let payload = serde_json::to_vec(cmd)
            .map_err(|e| remote_error(TalonRemoteErrorKind::Protocol, format!("encode: {e}")))?;
        let mut stream = self
            .stream
            .lock()
            .map_err(|_| remote_error(TalonRemoteErrorKind::Io, "connection lock poisoned"))?;
        write_remote_frame(&mut stream, &payload)?;
        let frame = read_remote_frame(&mut stream)?;
        serde_json::from_slice(&frame)
            .map_err(|e| remote_error(TalonRemoteErrorKind::Protocol, format!("decode: {e}")))
    }

    fn exec_cmd(&self, cmd: &serde_json::Value) -> Result<(), TalonError> {
        let resp = self.exec_cmd_json(cmd)?;
        remote_response_data(&resp)?;
        Ok(())
    }
}

/// Remote KV engine wrapper.
pub struct RemoteKvEngine<'a> {
    client: &'a TalonRemoteClient,
}

impl<'a> RemoteKvEngine<'a> {
    /// Read a key from remote KV.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, TalonError> {
        let key = remote_utf8("KV key", key)?;
        let cmd = serde_json::json!({
            "module": "kv",
            "action": "get",
            "params": { "key": key }
        });
        let resp = self.client.exec_cmd_json(&cmd)?;
        let data = remote_response_data(&resp)?.ok_or_else(|| {
            remote_error(
                TalonRemoteErrorKind::Protocol,
                format!("KV get response missing data: {resp}"),
            )
        })?;
        match data.get("value") {
            Some(v) if v.is_null() => Ok(None),
            Some(v) => v
                .as_str()
                .map(|s| Some(s.as_bytes().to_vec()))
                .ok_or_else(|| {
                    remote_error(
                        TalonRemoteErrorKind::Protocol,
                        format!("KV get value is not a string/null: {resp}"),
                    )
                }),
            None => Err(remote_error(
                TalonRemoteErrorKind::Protocol,
                format!("KV get response missing value: {resp}"),
            )),
        }
    }

    /// Write a remote KV value. The server JSON protocol stores string payloads.
    pub fn set(&self, key: &[u8], value: &[u8], ttl_secs: Option<u64>) -> Result<(), TalonError> {
        let key = remote_utf8("KV key", key)?;
        let value = remote_utf8("KV value", value)?;
        let mut params = serde_json::json!({ "key": key, "value": value });
        if let Some(ttl) = ttl_secs {
            params["ttl"] = serde_json::json!(ttl);
        }
        let cmd = serde_json::json!({
            "module": "kv",
            "action": "set",
            "params": params
        });
        self.client.exec_cmd(&cmd)
    }

    /// Delete a remote KV key.
    pub fn del(&self, key: &[u8]) -> Result<(), TalonError> {
        let key = remote_utf8("KV key", key)?;
        let cmd = serde_json::json!({
            "module": "kv",
            "action": "del",
            "params": { "key": key }
        });
        self.client.exec_cmd(&cmd)
    }
}

/// Remote MQ engine wrapper.
pub struct RemoteMqEngine<'a> {
    client: &'a TalonRemoteClient,
}

impl<'a> RemoteMqEngine<'a> {
    /// Create a remote topic.
    pub fn create_topic(&self, topic: &str, max_len: u64) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "mq",
            "action": "create",
            "params": { "topic": topic, "max_len": max_len }
        });
        self.client.exec_cmd(&cmd)
    }

    /// Publish a message to a remote topic.
    pub fn publish(&self, topic: &str, payload: &[u8]) -> Result<u64, TalonError> {
        let payload_str = String::from_utf8_lossy(payload);
        let cmd = serde_json::json!({
            "module": "mq",
            "action": "publish",
            "params": { "topic": topic, "payload": payload_str }
        });
        let resp = self.client.exec_cmd_json(&cmd)?;
        let data = remote_response_data(&resp)?;
        Ok(data
            .and_then(|d| d.get("id"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }

    /// Publish a delayed message to a remote topic.
    pub fn publish_delayed(
        &self,
        topic: &str,
        payload: &[u8],
        delay_ms: u64,
    ) -> Result<u64, TalonError> {
        let payload_str = String::from_utf8_lossy(payload);
        let cmd = serde_json::json!({
            "module": "mq",
            "action": "publish",
            "params": { "topic": topic, "payload": payload_str, "delay_ms": delay_ms }
        });
        let resp = self.client.exec_cmd_json(&cmd)?;
        let data = remote_response_data(&resp)?;
        Ok(data
            .and_then(|d| d.get("id"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }

    /// Acknowledge a remote MQ message.
    pub fn ack(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        message_id: u64,
    ) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "mq",
            "action": "ack",
            "params": {
                "topic": topic,
                "group": group,
                "consumer": consumer,
                "message_id": message_id
            }
        });
        self.client.exec_cmd(&cmd)
    }

    /// Poll remote MQ messages.
    pub fn poll(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        count: usize,
    ) -> Result<Vec<MqMessage>, TalonError> {
        let cmd = serde_json::json!({
            "module": "mq",
            "action": "poll",
            "params": {
                "topic": topic,
                "group": group,
                "consumer": consumer,
                "count": count
            }
        });
        let resp = self.client.exec_cmd_json(&cmd)?;
        let messages = remote_response_data(&resp)?
            .and_then(|d| d.get("messages"))
            .and_then(|m| m.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(messages
            .into_iter()
            .map(|m| MqMessage {
                id: m.get("id").and_then(|v| v.as_u64()).unwrap_or(0),
                payload: m
                    .get("payload")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .as_bytes()
                    .to_vec(),
                timestamp: m.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0),
            })
            .collect())
    }

    /// Subscribe a consumer group to a remote topic.
    pub fn subscribe(&self, topic: &str, group: &str) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "mq",
            "action": "subscribe",
            "params": { "topic": topic, "group": group }
        });
        self.client.exec_cmd(&cmd)
    }

    /// List remote topics.
    pub fn list_topics(&self) -> Result<Vec<String>, TalonError> {
        let cmd = serde_json::json!({
            "module": "mq",
            "action": "topics",
            "params": {}
        });
        let resp = self.client.exec_cmd_json(&cmd)?;
        Ok(remote_response_data(&resp)?
            .and_then(|d| d.get("topics"))
            .and_then(|t| t.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default())
    }
}

fn parse_talon_remote_endpoint(
    endpoint: &str,
    default_timeout: Duration,
) -> Result<RemoteEndpoint, TalonError> {
    let endpoint = endpoint.trim();
    let rest = endpoint.strip_prefix("talon://").ok_or_else(|| {
        remote_error(
            TalonRemoteErrorKind::InvalidEndpoint,
            "endpoint must start with talon://",
        )
    })?;
    let (authority, query) = rest.split_once('?').unwrap_or((rest, ""));
    if authority.is_empty() || authority.contains('/') {
        return Err(remote_error(
            TalonRemoteErrorKind::InvalidEndpoint,
            "endpoint must be talon://host:port",
        ));
    }

    let (userinfo, addr) = authority
        .rsplit_once('@')
        .map(|(u, a)| (Some(u), a))
        .unwrap_or((None, authority));
    if addr.is_empty() || !addr.contains(':') {
        return Err(remote_error(
            TalonRemoteErrorKind::InvalidEndpoint,
            "endpoint must include host:port",
        ));
    }

    let mut auth_token = userinfo.and_then(|u| {
        let token = u.split_once(':').map(|(_, password)| password).unwrap_or(u);
        (!token.is_empty()).then(|| token.to_string())
    });
    let mut timeout = default_timeout;
    if !query.is_empty() {
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            match key {
                "auth" | "auth_token" | "token" => {
                    if !value.is_empty() {
                        auth_token = Some(value.to_string());
                    }
                }
                "timeout_ms" => {
                    let millis = value.parse::<u64>().map_err(|e| {
                        remote_error(
                            TalonRemoteErrorKind::InvalidEndpoint,
                            format!("invalid timeout_ms value: {e}"),
                        )
                    })?;
                    timeout = Duration::from_millis(millis);
                }
                "timeout" | "timeout_secs" => {
                    let secs = value.parse::<u64>().map_err(|e| {
                        remote_error(
                            TalonRemoteErrorKind::InvalidEndpoint,
                            format!("invalid timeout value: {e}"),
                        )
                    })?;
                    timeout = Duration::from_secs(secs);
                }
                "protocol" if value != "tcp" => {
                    return Err(remote_error(
                        TalonRemoteErrorKind::InvalidEndpoint,
                        format!("unsupported protocol={value}; expected tcp"),
                    ));
                }
                "tls" if value == "true" || value == "1" => {
                    return Err(remote_error(
                        TalonRemoteErrorKind::InvalidEndpoint,
                        "TLS talon:// endpoints are not supported by this TCP client",
                    ));
                }
                _ => {}
            }
        }
    }
    if timeout.is_zero() {
        return Err(remote_error(
            TalonRemoteErrorKind::InvalidEndpoint,
            "timeout must be greater than zero",
        ));
    }

    Ok(RemoteEndpoint {
        endpoint: endpoint.to_string(),
        addr: addr.to_string(),
        auth_token,
        timeout,
    })
}

fn connect_remote_stream(endpoint: &RemoteEndpoint) -> Result<TcpStream, TalonError> {
    let addrs = endpoint.addr.to_socket_addrs().map_err(|e| {
        remote_error(
            TalonRemoteErrorKind::Connect,
            format!("resolve {}: {e}", endpoint.addr),
        )
    })?;
    let mut last_err: Option<std::io::Error> = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, endpoint.timeout) {
            Ok(stream) => {
                stream
                    .set_read_timeout(Some(endpoint.timeout))
                    .map_err(|e| {
                        remote_io_error(TalonRemoteErrorKind::Io, "set read timeout", e)
                    })?;
                stream
                    .set_write_timeout(Some(endpoint.timeout))
                    .map_err(|e| {
                        remote_io_error(TalonRemoteErrorKind::Io, "set write timeout", e)
                    })?;
                return Ok(stream);
            }
            Err(e) => last_err = Some(e),
        }
    }
    match last_err {
        Some(e) => Err(remote_io_error(
            TalonRemoteErrorKind::Connect,
            &format!("connect {}", endpoint.addr),
            e,
        )),
        None => Err(remote_error(
            TalonRemoteErrorKind::Connect,
            format!("no socket addresses resolved for {}", endpoint.addr),
        )),
    }
}

fn authenticate_remote_stream(stream: &mut TcpStream, token: &str) -> Result<(), TalonError> {
    let payload = serde_json::to_vec(&serde_json::json!({ "auth": token }))
        .map_err(|e| remote_error(TalonRemoteErrorKind::Protocol, format!("auth encode: {e}")))?;
    write_remote_frame(stream, &payload)?;
    let frame = read_remote_frame(stream)?;
    let resp: serde_json::Value = serde_json::from_slice(&frame).map_err(|e| {
        remote_error(
            TalonRemoteErrorKind::Handshake,
            format!("auth response decode: {e}"),
        )
    })?;
    match resp.get("ok").and_then(|v| v.as_bool()) {
        Some(true) => Ok(()),
        Some(false) => Err(remote_error(
            TalonRemoteErrorKind::Auth,
            resp.get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("auth failed"),
        )),
        None => Err(remote_error(
            TalonRemoteErrorKind::Handshake,
            format!("auth response missing ok: {resp}"),
        )),
    }
}

fn write_remote_frame(stream: &mut TcpStream, data: &[u8]) -> Result<(), TalonError> {
    if data.len() > MAX_REMOTE_FRAME_SIZE as usize {
        return Err(remote_error(
            TalonRemoteErrorKind::Protocol,
            format!(
                "frame size {} exceeds max {}",
                data.len(),
                MAX_REMOTE_FRAME_SIZE
            ),
        ));
    }
    let len = data.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .map_err(|e| remote_io_error(TalonRemoteErrorKind::Io, "write frame length", e))?;
    stream
        .write_all(data)
        .map_err(|e| remote_io_error(TalonRemoteErrorKind::Io, "write frame payload", e))?;
    stream
        .flush()
        .map_err(|e| remote_io_error(TalonRemoteErrorKind::Io, "flush frame", e))?;
    Ok(())
}

fn read_remote_frame(stream: &mut TcpStream) -> Result<Vec<u8>, TalonError> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .map_err(|e| remote_io_error(TalonRemoteErrorKind::Handshake, "read frame length", e))?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_REMOTE_FRAME_SIZE {
        return Err(remote_error(
            TalonRemoteErrorKind::Protocol,
            format!("frame size {len} exceeds max {MAX_REMOTE_FRAME_SIZE}"),
        ));
    }
    let mut buf = vec![0u8; len as usize];
    stream
        .read_exact(&mut buf)
        .map_err(|e| remote_io_error(TalonRemoteErrorKind::Protocol, "read frame payload", e))?;
    Ok(buf)
}

fn remote_response_data(
    resp: &serde_json::Value,
) -> Result<Option<&serde_json::Value>, TalonError> {
    match resp.get("ok").and_then(|v| v.as_bool()) {
        Some(true) => Ok(resp.get("data")),
        Some(false) => {
            let msg = resp
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown remote server error");
            let kind = if msg == "auth failed" {
                TalonRemoteErrorKind::Auth
            } else {
                TalonRemoteErrorKind::Server
            };
            Err(remote_error(kind, msg))
        }
        None => Err(remote_error(
            TalonRemoteErrorKind::Handshake,
            format!("response missing ok boolean: {resp}"),
        )),
    }
}

fn remote_utf8<'a>(field: &str, bytes: &'a [u8]) -> Result<&'a str, TalonError> {
    std::str::from_utf8(bytes).map_err(|e| {
        remote_error(
            TalonRemoteErrorKind::Protocol,
            format!("{field} must be valid UTF-8 for the current JSON TCP protocol: {e}"),
        )
    })
}

fn talon_value_from_json(value: &serde_json::Value) -> Result<Value, TalonError> {
    if let Ok(v) = serde_json::from_value::<Value>(value.clone()) {
        return Ok(v);
    }
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(v) => Ok(Value::Boolean(*v)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err(remote_error(
                    TalonRemoteErrorKind::Protocol,
                    format!("unsupported numeric value: {value}"),
                ))
            }
        }
        serde_json::Value::String(s) => Ok(Value::Text(s.clone())),
        other => Ok(Value::Jsonb(other.clone())),
    }
}

fn inline_sql_params(sql: &str, params: &[Value]) -> Result<String, TalonError> {
    let mut rendered = String::with_capacity(sql.len() + params.len() * 8);
    let mut param_idx = 0usize;
    let mut in_single_quote = false;
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            rendered.push(ch);
            if in_single_quote && chars.peek() == Some(&'\'') {
                rendered.push(chars.next().unwrap());
            } else {
                in_single_quote = !in_single_quote;
            }
            continue;
        }
        if ch == '?' && !in_single_quote {
            let param = params.get(param_idx).ok_or_else(|| {
                remote_error(
                    TalonRemoteErrorKind::Protocol,
                    "not enough SQL parameters for placeholders",
                )
            })?;
            rendered.push_str(&sql_literal(param)?);
            param_idx += 1;
        } else {
            rendered.push(ch);
        }
    }
    if param_idx != params.len() {
        return Err(remote_error(
            TalonRemoteErrorKind::Protocol,
            format!(
                "too many SQL parameters: consumed {}, provided {}",
                param_idx,
                params.len()
            ),
        ));
    }
    Ok(rendered)
}

fn sql_literal(value: &Value) -> Result<String, TalonError> {
    match value {
        Value::Null => Ok("NULL".into()),
        Value::Integer(v) | Value::Timestamp(v) => Ok(v.to_string()),
        Value::Float(v) if v.is_finite() => Ok(v.to_string()),
        Value::Float(_) => Err(remote_error(
            TalonRemoteErrorKind::Protocol,
            "non-finite floats cannot be rendered as SQL literals",
        )),
        Value::Text(v) => Ok(format!("'{}'", v.replace('\'', "''"))),
        Value::Blob(v) => Ok(format!(
            "X'{}'",
            v.iter().map(|b| format!("{b:02x}")).collect::<String>()
        )),
        Value::Boolean(v) => Ok(if *v { "TRUE".into() } else { "FALSE".into() }),
        Value::Jsonb(v) => Ok(format!("'{}'", v.to_string().replace('\'', "''"))),
        Value::Vector(v) => Ok(format!(
            "'{}'",
            serde_json::to_string(v)
                .map_err(|e| remote_error(TalonRemoteErrorKind::Protocol, e.to_string()))?
                .replace('\'', "''")
        )),
        Value::GeoPoint(lat, lon) => Ok(format!("'{lat},{lon}'")),
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
        pub fn talon_kv_del(handle: *const TalonHandle, key: *const u8, key_len: usize) -> c_int;
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

        // ── Server 管理 ──
        pub fn talon_start_server(handle: *const TalonHandle, tcp_addr: *const c_char) -> c_int;
        pub fn talon_stop_server(handle: *const TalonHandle) -> c_int;

        // ── 二进制 FFI（零 JSON 开销）──
        pub fn talon_run_sql_bin(
            handle: *const TalonHandle,
            sql: *const c_char,
            out_data: *mut *mut u8,
            out_len: *mut usize,
        ) -> c_int;
        pub fn talon_run_sql_param_bin(
            handle: *const TalonHandle,
            sql: *const c_char,
            params: *const u8,
            params_len: usize,
            out_data: *mut *mut u8,
            out_len: *mut usize,
        ) -> c_int;
        pub fn talon_vector_search_bin(
            handle: *const TalonHandle,
            index_name: *const c_char,
            vec_data: *const f32,
            vec_dim: usize,
            k: usize,
            metric: *const c_char,
            out_data: *mut *mut u8,
            out_len: *mut usize,
        ) -> c_int;
    }
}

// ── AI Handler 主动注册（替代 ctor，绕开 macOS dead-stripping）──────────────

extern "C" {
    /// talon-bundle 导出的显式 AI handler 注册入口。
    /// 由 talon-bundle/src/lib.rs 的 `#[no_mangle] pub extern "C" fn talon_bundle_init_ai()` 提供。
    /// 通过直接调用（而非 ctor）触发注册，链接器不会 dead-strip 直接引用的函数。
    fn talon_bundle_init_ai();
}

use std::sync::OnceLock;
static AI_INIT: OnceLock<()> = OnceLock::new();

/// 在 Talon::open 时调用，确保 AI handler 注册到路由器。
/// 幂等：多次调用只执行一次（OnceLock 保证）。
fn init_ai_handler() {
    AI_INIT.get_or_init(|| unsafe {
        talon_bundle_init_ai();
    });
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
        Ok(resp
            .get("data")
            .and_then(|d| d.get("deleted"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false))
    }
    /// BM25 全文搜索。
    pub fn search(
        &self,
        name: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>, TalonError> {
        let cmd = serde_json::json!({
            "module": "fts", "action": "search",
            "params": { "name": name, "query": query, "limit": limit }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let hits = resp
            .get("data")
            .and_then(|d| d.get("hits"))
            .and_then(|h| h.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|h| {
                        let doc_id = h.get("doc_id")?.as_str()?.to_string();
                        let score = h.get("score")?.as_f64()? as f32;
                        Some(SearchHit { doc_id, score })
                    })
                    .collect()
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
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        metric: &str,
    ) -> Result<Vec<(u64, f32)>, TalonError> {
        self.db.raw_vector_search(&self.index, query, k, metric)
    }
    /// 向量数量。
    pub fn count(&self) -> Result<u64, TalonError> {
        let cmd = serde_json::json!({
            "module": "vector", "action": "count",
            "params": { "index": &self.index }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }
}

// ── AI 类型 ─────────────────────────────────────────────────────────────────

/// Session 元数据。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub created_at: i64,
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub archived: bool,
    pub expires_at: Option<i64>,
}

/// 上下文消息（对话历史中的一条）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextMessage {
    pub role: String,
    pub content: String,
    pub timestamp: i64,
    pub token_count: Option<u32>,
}

/// AI 引擎包装（通过 execute JSON 命令代理）。
pub struct AiEngine<'a> {
    db: &'a Talon,
}

impl<'a> AiEngine<'a> {
    // ── Session ──

    /// 创建 Session。
    pub fn create_session(
        &self,
        id: &str,
        metadata: BTreeMap<String, String>,
        ttl_secs: Option<u64>,
    ) -> Result<Session, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "create_session",
            "params": { "id": id, "metadata": metadata, "ttl": ttl_secs }
        });
        self.db.exec_cmd(&cmd)?;
        // Return a minimal Session (engine doesn't echo back the full object)
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        Ok(Session {
            id: id.to_string(),
            created_at: now_secs,
            metadata,
            archived: false,
            expires_at: ttl_secs.map(|t| now_secs + t as i64),
        })
    }

    /// 获取 Session。
    pub fn get_session(&self, id: &str) -> Result<Option<Session>, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "get_session",
            "params": { "id": id }
        });
        let mut resp = self.db.exec_cmd_json(&cmd)?;
        let session = resp
            .get_mut("data")
            .and_then(|d| d.get_mut("session"))
            .and_then(|s| serde_json::from_value::<Session>(s.take()).ok());
        Ok(session)
    }

    /// 删除 Session（级联删除 context + trace）。
    pub fn delete_session(&self, id: &str) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "delete_session",
            "params": { "id": id }
        });
        self.db.exec_cmd(&cmd)
    }

    // ── Context ──

    /// 追加一条上下文消息。
    pub fn append_message(&self, session_id: &str, msg: &ContextMessage) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "append_message",
            "params": { "session_id": session_id, "message": msg }
        });
        self.db.exec_cmd(&cmd)
    }

    /// 获取对话历史（时间正序）。
    pub fn get_history(
        &self,
        session_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<ContextMessage>, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "get_history",
            "params": { "session_id": session_id, "limit": limit }
        });
        let mut resp = self.db.exec_cmd_json(&cmd)?;
        let msgs = resp
            .get_mut("data")
            .and_then(|d| d.get_mut("messages"))
            .and_then(|m| serde_json::from_value::<Vec<ContextMessage>>(m.take()).ok())
            .unwrap_or_default();
        Ok(msgs)
    }

    /// 获取最近 N 条消息（时间正序）。
    pub fn get_recent_messages(
        &self,
        session_id: &str,
        n: usize,
    ) -> Result<Vec<ContextMessage>, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "get_recent_messages",
            "params": { "session_id": session_id, "n": n }
        });
        let mut resp = self.db.exec_cmd_json(&cmd)?;
        let msgs = resp
            .get_mut("data")
            .and_then(|d| d.get_mut("messages"))
            .and_then(|m| serde_json::from_value::<Vec<ContextMessage>>(m.take()).ok())
            .unwrap_or_default();
        Ok(msgs)
    }

    /// 获取 token 预算窗口内的上下文（含 system_prompt + summary）。
    pub fn get_context_window_with_prompt(
        &self,
        session_id: &str,
        max_tokens: u32,
    ) -> Result<Vec<ContextMessage>, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "get_context_window_with_prompt",
            "params": { "session_id": session_id, "max_tokens": max_tokens }
        });
        let mut resp = self.db.exec_cmd_json(&cmd)?;
        let msgs = resp
            .get_mut("data")
            .and_then(|d| d.get_mut("messages"))
            .and_then(|m| serde_json::from_value::<Vec<ContextMessage>>(m.take()).ok())
            .unwrap_or_default();
        Ok(msgs)
    }

    /// 智能上下文窗口（含自动摘要压缩）。
    ///
    /// 当对话总 token 超过 `max_tokens × 2` 且 Chat Provider 已配置时，
    /// Talon AI Engine 自动调用 LLM 生成旧消息摘要，再返回截取的上下文。
    /// 未配置 LLM 则退化为普通截取。
    pub fn get_context_window_smart(
        &self,
        session_id: &str,
        max_tokens: u32,
    ) -> Result<Vec<ContextMessage>, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "get_context_window_smart",
            "params": { "session_id": session_id, "max_tokens": max_tokens }
        });
        let mut resp = self.db.exec_cmd_json(&cmd)?;
        let msgs = resp
            .get_mut("data")
            .and_then(|d| d.get_mut("messages"))
            .and_then(|m| serde_json::from_value::<Vec<ContextMessage>>(m.take()).ok())
            .unwrap_or_default();
        Ok(msgs)
    }

    /// 自动生成上下文摘要（需要已配置 Chat Provider）。
    ///
    /// 获取 session 的全部历史消息，调用 LLM 生成摘要，
    /// 存储到 session metadata 中。可选清理已摘要的旧消息。
    pub fn auto_summarize(
        &self,
        session_id: &str,
        max_summary_tokens: u32,
        purge_old: bool,
    ) -> Result<String, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "auto_summarize",
            "params": {
                "session_id": session_id,
                "max_summary_tokens": max_summary_tokens,
                "purge_old": purge_old,
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let summary = resp
            .get("data")
            .and_then(|d| d.get("summary"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Ok(summary)
    }

    /// 清空 Session 的全部上下文消息。
    pub fn clear_context(&self, session_id: &str) -> Result<u64, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "clear_context",
            "params": { "session_id": session_id }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let purged = resp
            .get("data")
            .and_then(|d| d.get("purged"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        Ok(purged)
    }

    // ── System Prompt / Context Summary ──

    /// 设置 Session 的 System Prompt。
    pub fn set_system_prompt(
        &self,
        session_id: &str,
        prompt: &str,
        token_count: u32,
    ) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "set_system_prompt",
            "params": { "session_id": session_id, "prompt": prompt, "token_count": token_count }
        });
        self.db.exec_cmd(&cmd)
    }

    /// 设置 Session 的上下文摘要。
    pub fn set_context_summary(
        &self,
        session_id: &str,
        summary: &str,
        token_count: u32,
    ) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "set_context_summary",
            "params": { "session_id": session_id, "summary": summary, "token_count": token_count }
        });
        self.db.exec_cmd(&cmd)
    }

    /// 获取 Session 的上下文摘要。
    pub fn get_context_summary(&self, session_id: &str) -> Result<Option<String>, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "get_context_summary",
            "params": { "session_id": session_id }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let summary = resp
            .get("data")
            .and_then(|d| d.get("summary"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        Ok(summary)
    }

    // ── Memory ──

    /// 存储一条记忆（含 embedding 向量）。
    pub fn store_memory(
        &self,
        content: &str,
        metadata: &BTreeMap<String, String>,
        embedding: &[f32],
    ) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "store_memory",
            "params": {
                "entry": { "content": content, "metadata": metadata },
                "embedding": embedding,
            }
        });
        self.db.exec_cmd(&cmd)
    }

    /// 语义搜索记忆。
    pub fn search_memory(
        &self,
        embedding: &[f32],
        k: usize,
    ) -> Result<serde_json::Value, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "search_memory",
            "params": { "embedding": embedding, "k": k }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("results"))
            .cloned()
            .unwrap_or(serde_json::json!([])))
    }

    /// 删除一条记忆。
    pub fn delete_memory(&self, id: u64) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "delete_memory",
            "params": { "id": id }
        });
        self.db.exec_cmd(&cmd)
    }

    /// 获取记忆总数。
    pub fn memory_count(&self) -> Result<u64, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "memory_count",
            "params": {}
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }

    /// 更新记忆内容和/或元数据。
    pub fn update_memory(
        &self,
        id: u64,
        content: Option<&str>,
        metadata: Option<&BTreeMap<String, String>>,
    ) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "update_memory",
            "params": { "id": id, "content": content, "metadata": metadata }
        });
        self.db.exec_cmd(&cmd)
    }

    // ── Memory: Hybrid (推荐) ──

    /// 存储记忆（自动 embed + 向量写 + FTS 索引 + 缓存 + 可选 EDU 提取）。
    ///
    /// 自动完成以下操作：
    /// 1. 调用 Embedding API 生成向量（自带 FNV 哈希缓存）
    /// 2. 写入向量索引（语义搜索）
    /// 3. 写入 FTS 索引（关键词搜索）
    /// 4. 存储元数据到 KV
    /// 5. [可选] 用 LLM 提取 EDU（结构化事件单元），每个 EDU 独立 embed + 存储
    ///
    /// 需要先调用 `set_llm_config` 配置 embed provider。
    /// 开启 `extract_facts` 时还需要 chat provider。
    pub fn add_memory(
        &self,
        content: &str,
        metadata: &BTreeMap<String, String>,
        ttl_secs: Option<u64>,
        extract_facts: bool,
    ) -> Result<u64, TalonError> {
        let mut params = serde_json::json!({
            "content": content,
            "metadata": metadata,
        });
        if let Some(ttl) = ttl_secs {
            params["ttl_secs"] = serde_json::json!(ttl);
        }
        if extract_facts {
            params["extract_facts"] = serde_json::json!(true);
        }
        let cmd = serde_json::json!({
            "module": "ai", "action": "add_memory",
            "params": params
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("id"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }

    /// 智能召回（hybrid search: BM25 + 向量，RRF 融合）。
    ///
    /// 两路检索融合：
    /// - FTS BM25 路：关键词精确匹配
    /// - Vector 路：语义相似度
    /// - RRF 融合排序：基于排名融合
    ///
    /// `temporal_boost`: 时间感知权重（0.0 = 关闭，推荐 0.3）。
    /// `metadata_filter`: 可选的 metadata 过滤条件（JSON 序列化的 MetadataFilter 枚举）。
    ///
    /// 需要先调用 `set_llm_config` 配置 embed provider。
    pub fn recall(
        &self,
        query: &str,
        k: usize,
        fts_weight: f64,
        vec_weight: f64,
        temporal_boost: f64,
        rerank: bool,
        rerank_top_k: Option<usize>,
        graph_depth: usize,
        metadata_filter: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, TalonError> {
        let mut params = serde_json::json!({
            "query": query, "k": k,
            "fts_weight": fts_weight, "vec_weight": vec_weight,
        });
        if temporal_boost > 0.0 {
            params["temporal_boost"] = serde_json::json!(temporal_boost);
        }
        if rerank {
            params["rerank"] = serde_json::json!(true);
        }
        if let Some(rtk) = rerank_top_k {
            params["rerank_top_k"] = serde_json::json!(rtk);
        }
        if graph_depth > 0 {
            params["graph_depth"] = serde_json::json!(graph_depth);
        }
        if let Some(filter) = metadata_filter {
            params["metadata_filter"] = filter.clone();
        }
        let cmd = serde_json::json!({
            "module": "ai", "action": "recall",
            "params": params
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("results"))
            .cloned()
            .unwrap_or(serde_json::json!([])))
    }

    // ── LLM Config ──

    /// 配置引擎内置的 LLM/Embedding Provider。
    pub fn set_llm_config(&self, config: &serde_json::Value) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "set_llm_config",
            "params": { "config": config }
        });
        self.db.exec_cmd(&cmd)
    }

    /// 记录 LLM 调用追踪。
    pub fn log_trace(&self, record: &serde_json::Value) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "log_trace",
            "params": { "record": record }
        });
        self.db.exec_cmd(&cmd)
    }

    /// 按 Session 查询 traces。
    pub fn query_traces_by_session(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "query_traces_by_session",
            "params": { "session_id": session_id }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("traces"))
            .cloned()
            .unwrap_or(serde_json::json!([])))
    }

    /// 按 Run ID 查询 traces。
    pub fn query_traces_by_run(&self, run_id: &str) -> Result<serde_json::Value, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "query_traces_by_run",
            "params": { "run_id": run_id }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("traces"))
            .cloned()
            .unwrap_or(serde_json::json!([])))
    }

    /// Trace 聚合统计。
    pub fn trace_stats(&self, session_id: Option<&str>) -> Result<serde_json::Value, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": "trace_stats",
            "params": { "session_id": session_id }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("stats"))
            .cloned()
            .unwrap_or(serde_json::json!({})))
    }

    // ── 通用 AI 命令执行 ──

    /// 执行任意 AI 模块命令（不关心返回值）。
    pub fn exec_ai_action(
        &self,
        action: &str,
        params: &serde_json::Value,
    ) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": action, "params": params
        });
        self.db.exec_cmd(&cmd)
    }

    /// 执行任意 AI 模块命令，返回 data 部分。
    pub fn query_ai_action(
        &self,
        action: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value, TalonError> {
        let cmd = serde_json::json!({
            "module": "ai", "action": action, "params": params
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp.get("data").cloned().unwrap_or(serde_json::json!({})))
    }
}

/// MQ 消息（从 Talon MQ Engine 拉取）。
#[derive(Debug, Clone)]
pub struct MqMessage {
    /// 消息 ID（递增）。
    pub id: u64,
    /// 消息载荷。
    pub payload: Vec<u8>,
    /// 发布时间戳（ms）。
    pub timestamp: u64,
}

/// Graph 顶点。
#[derive(Debug, Clone)]
pub struct GraphVertex {
    pub id: u64,
    pub label: String,
    pub properties: std::collections::BTreeMap<String, String>,
}

/// Graph 边。
#[derive(Debug, Clone)]
pub struct GraphEdge {
    pub id: u64,
    pub from: u64,
    pub to: u64,
    pub label: String,
    pub properties: std::collections::BTreeMap<String, String>,
}

/// Graph 遍历方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphDirection {
    /// 仅出边方向。
    Out,
    /// 仅入边方向。
    In,
    /// 双向（出边 + 入边）。
    Both,
}

/// Graph 引擎包装（通过 talon_execute JSON 命令代理）。
pub struct GraphEngine<'a> {
    db: &'a Talon,
}

impl<'a> GraphEngine<'a> {
    /// 创建图（幂等）。
    pub fn create(&self, graph: &str) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "graph", "action": "create",
            "params": { "graph": graph }
        });
        self.db.exec_cmd(&cmd)
    }

    /// 添加顶点，返回 vertex_id。
    pub fn add_vertex(
        &self,
        graph: &str,
        label: &str,
        properties: &std::collections::BTreeMap<String, String>,
    ) -> Result<u64, TalonError> {
        let cmd = serde_json::json!({
            "module": "graph", "action": "add_vertex",
            "params": { "graph": graph, "label": label, "properties": properties }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("vertex_id"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }

    /// 更新顶点属性。
    pub fn update_vertex(
        &self,
        graph: &str,
        id: u64,
        properties: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "graph", "action": "update_vertex",
            "params": { "graph": graph, "id": id, "properties": properties }
        });
        self.db.exec_cmd(&cmd)
    }

    /// 获取顶点。
    pub fn get_vertex(&self, graph: &str, id: u64) -> Result<Option<GraphVertex>, TalonError> {
        let cmd = serde_json::json!({
            "module": "graph", "action": "get_vertex",
            "params": { "graph": graph, "id": id }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let data = resp.get("data");
        if data.map(|d| d.is_null()).unwrap_or(true) {
            return Ok(None);
        }
        Ok(data.map(|d| parse_vertex(d)))
    }

    /// 按 label 查顶点。
    pub fn vertices_by_label(
        &self,
        graph: &str,
        label: &str,
    ) -> Result<Vec<GraphVertex>, TalonError> {
        let cmd = serde_json::json!({
            "module": "graph", "action": "vertices_by_label",
            "params": { "graph": graph, "label": label }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let arr = resp
            .get("data")
            .and_then(|d| d.get("vertices"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(arr.iter().map(parse_vertex).collect())
    }

    /// 添加边，返回 edge_id。
    pub fn add_edge(
        &self,
        graph: &str,
        from: u64,
        to: u64,
        label: &str,
        properties: &std::collections::BTreeMap<String, String>,
    ) -> Result<u64, TalonError> {
        let cmd = serde_json::json!({
            "module": "graph", "action": "add_edge",
            "params": {
                "graph": graph, "from": from, "to": to,
                "label": label, "properties": properties
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("edge_id"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }

    /// 获取节点的出边。
    pub fn out_edges(&self, graph: &str, id: u64) -> Result<Vec<GraphEdge>, TalonError> {
        let cmd = serde_json::json!({
            "module": "graph", "action": "out_edges",
            "params": { "graph": graph, "id": id }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let arr = resp
            .get("data")
            .and_then(|d| d.get("edges"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(arr.iter().map(parse_edge).collect())
    }

    /// 获取顶点数。
    pub fn vertex_count(&self, graph: &str) -> Result<u64, TalonError> {
        let cmd = serde_json::json!({
            "module": "graph", "action": "vertex_count",
            "params": { "graph": graph }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }

    /// 获取节点的入边。
    pub fn in_edges(&self, graph: &str, id: u64) -> Result<Vec<GraphEdge>, TalonError> {
        let cmd = serde_json::json!({
            "module": "graph", "action": "in_edges",
            "params": { "graph": graph, "id": id }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let arr = resp
            .get("data")
            .and_then(|d| d.get("edges"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(arr.iter().map(parse_edge).collect())
    }

    /// 删除顶点（级联删除关联边）。
    pub fn delete_vertex(&self, graph: &str, id: u64) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "graph", "action": "delete_vertex",
            "params": { "graph": graph, "id": id }
        });
        self.db.exec_cmd(&cmd)
    }

    /// 获取边数。
    pub fn edge_count(&self, graph: &str) -> Result<u64, TalonError> {
        let cmd = serde_json::json!({
            "module": "graph", "action": "edge_count",
            "params": { "graph": graph }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }

    /// 获取邻居节点 ID（按方向过滤）。
    pub fn neighbors(
        &self,
        graph: &str,
        vertex_id: u64,
        direction: GraphDirection,
    ) -> Result<Vec<u64>, TalonError> {
        let dir_str = match direction {
            GraphDirection::Out => "out",
            GraphDirection::In => "in",
            GraphDirection::Both => "both",
        };
        let cmd = serde_json::json!({
            "module": "graph", "action": "neighbors",
            "params": { "graph": graph, "id": vertex_id, "direction": dir_str }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let ids = resp
            .get("data")
            .and_then(|d| d.get("neighbors"))
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_u64()).collect())
            .unwrap_or_default();
        Ok(ids)
    }
}

/// 解析 JSON 到 GraphVertex。
fn parse_vertex(v: &serde_json::Value) -> GraphVertex {
    GraphVertex {
        id: v.get("id").and_then(|x| x.as_u64()).unwrap_or(0),
        label: v
            .get("label")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string(),
        properties: v
            .get("properties")
            .and_then(|x| x.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, val)| {
                        (
                            k.clone(),
                            val.as_str().unwrap_or(&val.to_string()).to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

/// 解析 JSON 到 GraphEdge。
fn parse_edge(v: &serde_json::Value) -> GraphEdge {
    GraphEdge {
        id: v.get("id").and_then(|x| x.as_u64()).unwrap_or(0),
        from: v.get("from").and_then(|x| x.as_u64()).unwrap_or(0),
        to: v.get("to").and_then(|x| x.as_u64()).unwrap_or(0),
        label: v
            .get("label")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string(),
        properties: v
            .get("properties")
            .and_then(|x| x.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, val)| {
                        (
                            k.clone(),
                            val.as_str().unwrap_or(&val.to_string()).to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

/// MQ 引擎包装（通过 talon_execute JSON 命令代理）。
pub struct MqEngine<'a> {
    db: &'a Talon,
}

impl<'a> MqEngine<'a> {
    /// 创建 topic（幂等：已存在时不报错，但不会重置 next_id）。
    pub fn create_topic(&self, topic: &str, max_len: u64) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "mq", "action": "create",
            "params": { "topic": topic, "max_len": max_len }
        });
        self.db.exec_cmd(&cmd)
    }

    /// 发布消息到 topic，返回消息 ID。
    pub fn publish(&self, topic: &str, payload: &[u8]) -> Result<u64, TalonError> {
        let payload_str = String::from_utf8_lossy(payload);
        let cmd = serde_json::json!({
            "module": "mq", "action": "publish",
            "params": { "topic": topic, "payload": payload_str }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("id"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }

    /// 发布延迟消息，返回消息 ID。
    pub fn publish_delayed(
        &self,
        topic: &str,
        payload: &[u8],
        delay_ms: u64,
    ) -> Result<u64, TalonError> {
        let payload_str = String::from_utf8_lossy(payload);
        let cmd = serde_json::json!({
            "module": "mq", "action": "publish",
            "params": { "topic": topic, "payload": payload_str, "delay_ms": delay_ms }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("id"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }

    /// 确认消息已消费。
    pub fn ack(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        message_id: u64,
    ) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "mq", "action": "ack",
            "params": {
                "topic": topic, "group": group,
                "consumer": consumer, "message_id": message_id
            }
        });
        self.db.exec_cmd(&cmd)
    }

    /// 拉取消息（非阻塞），返回消息列表。
    pub fn poll(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        count: usize,
    ) -> Result<Vec<MqMessage>, TalonError> {
        let cmd = serde_json::json!({
            "module": "mq", "action": "poll",
            "params": {
                "topic": topic, "group": group,
                "consumer": consumer, "count": count
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let messages = resp
            .get("data")
            .and_then(|d| d.get("messages"))
            .and_then(|m| m.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(messages
            .into_iter()
            .map(|m| MqMessage {
                id: m.get("id").and_then(|v| v.as_u64()).unwrap_or(0),
                payload: m
                    .get("payload")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .as_bytes()
                    .to_vec(),
                timestamp: m.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0),
            })
            .collect())
    }

    /// 订阅 consumer group 到 topic（幂等）。
    pub fn subscribe(&self, topic: &str, group: &str) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "mq", "action": "subscribe",
            "params": { "topic": topic, "group": group }
        });
        self.db.exec_cmd(&cmd)
    }

    /// 列出所有 topic 名称。
    pub fn list_topics(&self) -> Result<Vec<String>, TalonError> {
        let cmd = serde_json::json!({
            "module": "mq", "action": "topics", "params": {}
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        Ok(resp
            .get("data")
            .and_then(|d| d.get("topics"))
            .and_then(|t| t.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default())
    }
}

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
        // 在打开数据库前，显式触发 AI handler 注册。
        // talon_bundle_init_ai 是 talon-bundle 导出的 #[no_mangle] C ABI 函数，
        // 链接器不会 dead-strip 被直接引用的函数（只有未被引用的 ctor 内容才会被剔除）。
        init_ai_handler();
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
        let s = path
            .to_str()
            .ok_or_else(|| TalonError("Invalid UTF-8 path".into()))?;
        Self::open(s)
    }

    /// Open an anonymous (temp directory) database, useful for tests.
    pub fn open_anon() -> Result<Self, TalonError> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("talon_anon_{}_{n}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        Self::open(dir.to_string_lossy().as_ref())
    }

    /// Connect to a remote Talon server endpoint, e.g. `talon://127.0.0.1:7720`.
    pub fn connect_remote(endpoint: &str) -> Result<TalonRemoteClient, TalonError> {
        TalonRemoteClient::connect(endpoint)
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
    pub fn run_sql_param(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<Vec<Value>>, TalonError> {
        let c_sql = CString::new(sql)?;
        let params_bin = encode_params(params);
        let mut out_data: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let rc = unsafe {
            raw_ffi::talon_run_sql_param_bin(
                self.handle,
                c_sql.as_ptr(),
                params_bin.as_ptr(),
                params_bin.len(),
                &mut out_data,
                &mut out_len,
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
        Ok(VectorEngine {
            db: self,
            index: index.to_string(),
        })
    }
    /// 获取 Vector 引擎（读）。
    pub fn vector_read(&self, index: &str) -> Result<VectorEngine<'_>, TalonError> {
        Ok(VectorEngine {
            db: self,
            index: index.to_string(),
        })
    }
    /// 获取 AI 引擎。
    pub fn ai(&self) -> Result<AiEngine<'_>, TalonError> {
        Ok(AiEngine { db: self })
    }
    /// 获取 AI 引擎（读）。
    pub fn ai_read(&self) -> Result<AiEngine<'_>, TalonError> {
        Ok(AiEngine { db: self })
    }
    /// 获取 MQ 引擎（写）。
    pub fn mq(&self) -> Result<MqEngine<'_>, TalonError> {
        Ok(MqEngine { db: self })
    }
    /// 获取 MQ 引擎（读）。
    pub fn mq_read(&self) -> Result<MqEngine<'_>, TalonError> {
        Ok(MqEngine { db: self })
    }
    /// 获取 Graph 引擎（写）。
    pub fn graph(&self) -> Result<GraphEngine<'_>, TalonError> {
        Ok(GraphEngine { db: self })
    }
    /// 获取 Graph 引擎（读）。
    pub fn graph_read(&self) -> Result<GraphEngine<'_>, TalonError> {
        Ok(GraphEngine { db: self })
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

    /// 启动后台 TCP Server，供外部客户端工具连接。
    ///
    /// `addr` 为监听地址，如 `"127.0.0.1:7720"`。
    /// 同一句柄只能启动一个 server，重复调用返回错误。
    pub fn start_server(&self, addr: &str) -> Result<(), TalonError> {
        let c_addr = CString::new(addr)?;
        let rc = unsafe { raw_ffi::talon_start_server(self.handle, c_addr.as_ptr()) };
        match rc {
            0 => Ok(()),
            -2 => Err(TalonError("Server already running".into())),
            _ => Err(TalonError("start_server FFI failed".into())),
        }
    }

    /// 停止后台 TCP Server。
    pub fn stop_server(&self) -> Result<(), TalonError> {
        let rc = unsafe { raw_ffi::talon_stop_server(self.handle) };
        if rc != 0 {
            return Err(TalonError("No server running".into()));
        }
        Ok(())
    }

    // ── 内部 FFI 辅助 ──

    fn raw_kv_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, TalonError> {
        let mut out_ptr: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let rc = unsafe {
            raw_ffi::talon_kv_get(
                self.handle,
                key.as_ptr(),
                key.len(),
                &mut out_ptr,
                &mut out_len,
            )
        };
        if rc != 0 {
            return Err(TalonError("kv_get FFI failed".into()));
        }
        if out_ptr.is_null() {
            return Ok(None);
        }
        let data = unsafe { slice::from_raw_parts(out_ptr, out_len).to_vec() };
        unsafe { raw_ffi::talon_free_bytes(out_ptr, out_len) };
        Ok(Some(data))
    }

    fn raw_kv_set(&self, key: &[u8], value: &[u8], ttl_secs: i64) -> Result<(), TalonError> {
        let rc = unsafe {
            raw_ffi::talon_kv_set(
                self.handle,
                key.as_ptr(),
                key.len(),
                value.as_ptr(),
                value.len(),
                ttl_secs,
            )
        };
        if rc != 0 {
            return Err(TalonError("kv_set FFI failed".into()));
        }
        Ok(())
    }

    fn raw_kv_del(&self, key: &[u8]) -> Result<(), TalonError> {
        let rc = unsafe { raw_ffi::talon_kv_del(self.handle, key.as_ptr(), key.len()) };
        if rc != 0 {
            return Err(TalonError("kv_del FFI failed".into()));
        }
        Ok(())
    }

    fn raw_vector_insert(&self, index: &str, id: u64, vec: &[f32]) -> Result<(), TalonError> {
        let c_name = CString::new(index)?;
        let rc = unsafe {
            raw_ffi::talon_vector_insert(self.handle, c_name.as_ptr(), id, vec.as_ptr(), vec.len())
        };
        if rc != 0 {
            return Err(TalonError("vector_insert FFI failed".into()));
        }
        Ok(())
    }

    fn raw_vector_search(
        &self,
        index: &str,
        query: &[f32],
        k: usize,
        metric: &str,
    ) -> Result<Vec<(u64, f32)>, TalonError> {
        let c_name = CString::new(index)?;
        let c_metric = CString::new(metric)?;
        let mut out_data: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let rc = unsafe {
            raw_ffi::talon_vector_search_bin(
                self.handle,
                c_name.as_ptr(),
                query.as_ptr(),
                query.len(),
                k,
                c_metric.as_ptr(),
                &mut out_data,
                &mut out_len,
            )
        };
        if rc != 0 {
            return Err(TalonError("vector_search FFI failed".into()));
        }
        if out_data.is_null() || out_len == 0 {
            return Ok(vec![]);
        }
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
            let msg = resp
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            Err(TalonError(msg.to_string()))
        }
    }

    /// 执行 JSON 命令，返回解析后的响应。
    pub fn exec_cmd_json(&self, cmd: &serde_json::Value) -> Result<serde_json::Value, TalonError> {
        let cmd_str = cmd.to_string();
        let c_cmd = CString::new(cmd_str)?;
        let mut out: *mut std::os::raw::c_char = ptr::null_mut();
        let rc = unsafe { raw_ffi::talon_execute(self.handle, c_cmd.as_ptr(), &mut out) };
        if rc != 0 {
            return Err(TalonError("execute FFI failed".into()));
        }
        if out.is_null() {
            return Err(TalonError("execute returned null output".into()));
        }
        let json_str = unsafe { CStr::from_ptr(out).to_string_lossy().into_owned() };
        unsafe { raw_ffi::talon_free_string(out) };
        serde_json::from_str(&json_str).map_err(|e| TalonError(format!("JSON parse: {e}")))
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
        Value::Integer(i) => {
            buf.push(1);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Value::Float(f) => {
            buf.push(2);
            buf.extend_from_slice(&f.to_le_bytes());
        }
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
        Value::Boolean(b) => {
            buf.push(5);
            buf.push(if *b { 1 } else { 0 });
        }
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
            for f in v {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        Value::Timestamp(t) => {
            buf.push(8);
            buf.extend_from_slice(&t.to_le_bytes());
        }
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
        let id = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
        let dist = f32::from_le_bytes(data[off + 8..off + 12].try_into().unwrap());
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
            if off + 8 > data.len() {
                return Err(TalonError("truncated i64".into()));
            }
            let v = i64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            Ok((Value::Integer(v), 9))
        }
        2 => {
            if off + 8 > data.len() {
                return Err(TalonError("truncated f64".into()));
            }
            let v = f64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            Ok((Value::Float(v), 9))
        }
        3 => {
            if off + 4 > data.len() {
                return Err(TalonError("truncated text len".into()));
            }
            let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
            let start = off + 4;
            if start + len > data.len() {
                return Err(TalonError("truncated text data".into()));
            }
            let s = std::str::from_utf8(&data[start..start + len])
                .map_err(|_| TalonError("invalid utf8 in text".into()))?;
            Ok((Value::Text(s.to_string()), 5 + len))
        }
        4 => {
            if off + 4 > data.len() {
                return Err(TalonError("truncated blob len".into()));
            }
            let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
            let start = off + 4;
            if start + len > data.len() {
                return Err(TalonError("truncated blob data".into()));
            }
            Ok((Value::Blob(data[start..start + len].to_vec()), 5 + len))
        }
        5 => {
            if off >= data.len() {
                return Err(TalonError("truncated bool".into()));
            }
            Ok((Value::Boolean(data[off] != 0), 2))
        }
        6 => {
            if off + 4 > data.len() {
                return Err(TalonError("truncated jsonb len".into()));
            }
            let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
            let start = off + 4;
            if start + len > data.len() {
                return Err(TalonError("truncated jsonb data".into()));
            }
            let s = std::str::from_utf8(&data[start..start + len])
                .map_err(|_| TalonError("invalid utf8 in jsonb".into()))?;
            let j: serde_json::Value =
                serde_json::from_str(s).map_err(|e| TalonError(format!("jsonb parse: {e}")))?;
            Ok((Value::Jsonb(j), 5 + len))
        }
        7 => {
            if off + 4 > data.len() {
                return Err(TalonError("truncated vec dim".into()));
            }
            let dim = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
            let start = off + 4;
            let byte_len = dim * 4;
            if start + byte_len > data.len() {
                return Err(TalonError("truncated vec data".into()));
            }
            let mut v = Vec::with_capacity(dim);
            for i in 0..dim {
                let s = start + i * 4;
                v.push(f32::from_le_bytes(data[s..s + 4].try_into().unwrap()));
            }
            Ok((Value::Vector(v), 5 + byte_len))
        }
        8 => {
            if off + 8 > data.len() {
                return Err(TalonError("truncated timestamp".into()));
            }
            let v = i64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            Ok((Value::Timestamp(v), 9))
        }
        9 => {
            if off + 16 > data.len() {
                return Err(TalonError("truncated geopoint".into()));
            }
            let lat = f64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            let lon = f64::from_le_bytes(data[off + 8..off + 16].try_into().unwrap());
            Ok((Value::GeoPoint(lat, lon), 17))
        }
        _ => Err(TalonError(format!("unknown binary type tag: {tag}"))),
    }
}

#[cfg(test)]
mod remote_tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;
    use std::time::{Duration, Instant};

    fn reserve_tcp_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        addr
    }

    fn connect_with_retry(endpoint: &str) -> Result<TalonRemoteClient, TalonError> {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut last_err = None;
        while Instant::now() < deadline {
            match TalonRemoteClient::connect_with_timeout(endpoint, Duration::from_millis(250)) {
                Ok(client) => return Ok(client),
                Err(err) => {
                    last_err = Some(err);
                    thread::sleep(Duration::from_millis(25));
                }
            }
        }
        Err(last_err.unwrap_or_else(|| TalonError("remote connect retry exhausted".into())))
    }

    #[test]
    fn remote_endpoint_requires_talon_scheme() {
        let err = TalonRemoteClient::connect("http://127.0.0.1:7720").unwrap_err();
        assert!(err.0.contains("remote invalid-endpoint"));
    }

    #[test]
    fn remote_auth_failure_is_classified() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = read_remote_frame(&mut stream).unwrap();
            write_remote_frame(&mut stream, br#"{"ok":false,"error":"auth failed"}"#).unwrap();
        });

        let err =
            TalonRemoteClient::connect(&format!("talon://{addr}?auth_token=bad")).unwrap_err();
        assert!(err.0.contains("remote auth: auth failed"));
        handle.join().unwrap();
    }

    #[test]
    fn remote_client_sql_kv_mq_roundtrip() {
        let db = Talon::open_anon().unwrap();
        let addr = reserve_tcp_addr();
        db.start_server(&addr).unwrap();

        let client = connect_with_retry(&format!("talon://{addr}")).unwrap();

        client
            .run_sql("CREATE TABLE remote_roundtrip (id INT, name TEXT)")
            .unwrap();
        client
            .run_sql("INSERT INTO remote_roundtrip VALUES (1, 'alice')")
            .unwrap();
        client
            .run_sql_param(
                "INSERT INTO remote_roundtrip VALUES (?, ?)",
                &[Value::Integer(2), Value::Text("bob".into())],
            )
            .unwrap();
        let rows = client
            .run_sql("SELECT name FROM remote_roundtrip WHERE id = 2")
            .unwrap();
        assert_eq!(rows, vec![vec![Value::Text("bob".into())]]);

        let kv = client.kv().unwrap();
        kv.set(b"remote:key", b"remote-value", None).unwrap();
        assert_eq!(
            kv.get(b"remote:key").unwrap(),
            Some(b"remote-value".to_vec())
        );
        kv.del(b"remote:key").unwrap();
        assert_eq!(kv.get(b"remote:key").unwrap(), None);

        let mq = client.mq().unwrap();
        mq.create_topic("remote-topic", 100).unwrap();
        mq.subscribe("remote-topic", "remote-group").unwrap();
        let msg_id = mq.publish("remote-topic", b"remote-message").unwrap();
        assert!(msg_id > 0);
        let messages = mq
            .poll("remote-topic", "remote-group", "remote-consumer", 1)
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].payload, b"remote-message");
        mq.ack(
            "remote-topic",
            "remote-group",
            "remote-consumer",
            messages[0].id,
        )
        .unwrap();
        assert!(mq
            .list_topics()
            .unwrap()
            .contains(&"remote-topic".to_string()));

        drop(client);
        // The current FFI stop_server joins a blocking acceptor thread. Let the
        // test process tear down the background server after proving roundtrip.
        std::mem::forget(db);
    }
}

// ── EvoCore 封装（条件编译）──────────────────────────────────────────────────
//
// 启用 `evocore` feature 后，通过 `module:"evo"` 命令访问 EvoCore 进化引擎。
// 预编译库需使用 libtalon-evocore.a（包含 talon + talon-ai + evo-core）。

#[cfg(feature = "evocore")]
mod evocore;

#[cfg(feature = "evocore")]
pub use evocore::*;
