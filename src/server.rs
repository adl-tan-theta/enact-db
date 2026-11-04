//! HTTP + WebSocket surface.
//!
//! | Area        | Endpoint                                                  |
//! |-------------|-----------------------------------------------------------|
//! | SQL         | `POST /v1/query` `{sql, format: "json"|"arrow"}`          |
//! | Agent FS    | `GET/PUT/DELETE /v1/agents/{id}/fs/{*path}`               |
//! | Agent KV    | `GET/PUT/DELETE /v1/agents/{id}/kv/{key}`                 |
//! | Agent audit | `GET/POST /v1/agents/{id}/tools`                          |
//! | Sandbox     | `POST /v1/agents/{id}/push` · `/pull` (blob tier)         |
//! | CRDT        | `GET /ws/{doc}` (y-websocket) · `/v1/docs/{doc}/...`      |
//! | Memory      | `POST /v1/memory` · `/v1/memory/search` · `DELETE ...`    |
//! | Transcripts | `POST /v1/transcripts` · `GET /v1/transcripts/{session}`  |
//! | Artifacts   | `GET/PUT /v1/blob/{*path}` · `GET /v1/blob`               |
//!
//! Callers identify themselves with `X-Agent-Id: <id>` (agents) and/or
//! `X-Actor: <name>` (humans). Agent writes are audited into that agent's
//! AgentFS `tool_calls` table.

use crate::agents::{self, now_secs, AgentRegistry};
use crate::blob::Blob;
use crate::config::ServerConfig;
use crate::crdt::{Frame, Hub};
use crate::memory::{MemoryQuery, MemoryRecord, MemoryStore};
use crate::store::{HubStore, TranscriptEntry};
use axum::body::Bytes;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use base64::Engine;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SQLOptions;
use datafusion::prelude::SessionContext;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use yrs::types::ToJson;
use yrs::{Array, GetString, Map, ReadTxn, Text, Transact};

pub struct AppState {
    pub ctx: SessionContext,
    pub agents: Arc<AgentRegistry>,
    pub hub: Arc<Hub>,
    pub store: Arc<HubStore>,
    pub memory: Arc<MemoryStore>,
    pub blob: Option<Arc<Blob>>,
    pub cfg: ServerConfig,
    pub sources: Vec<String>,
    pub sinks: Vec<String>,
    pub peer_seq: AtomicU64,
    pub quack: Arc<crate::quack::QuackServer>,
}

type St = State<Arc<AppState>>;

// ---- errors ---------------------------------------------------------------------

pub struct ApiError(StatusCode, String);

impl ApiError {
    pub fn bad(msg: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, msg.into())
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self(StatusCode::NOT_FOUND, msg.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        let code = StatusCode::from_u16(agents::status_for(&e)).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        ApiError(code, format!("{e:#}"))
    }
}

impl From<datafusion::error::DataFusionError> for ApiError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        ApiError(StatusCode::BAD_REQUEST, e.to_string())
    }
}

impl From<agentfs_sdk::error::Error> for ApiError {
    fn from(e: agentfs_sdk::error::Error) -> Self {
        anyhow::Error::from(e).into()
    }
}

type ApiResult<T> = Result<T, ApiError>;

fn agent_header(h: &HeaderMap) -> Option<String> {
    h.get("x-agent-id").and_then(|v| v.to_str().ok()).map(|s| s.to_string())
}

fn actor_header(h: &HeaderMap) -> Option<String> {
    h.get("x-actor").and_then(|v| v.to_str().ok()).map(|s| s.to_string())
}

/// CRDT origin tag for an HTTP caller.
fn http_origin(h: &HeaderMap) -> String {
    match (agent_header(h), actor_header(h)) {
        (Some(a), _) => format!("agent:{a}"),
        (None, Some(u)) => format!("human:{u}"),
        _ => "http".into(),
    }
}

// ---- router -----------------------------------------------------------------------

pub fn router(state: Arc<AppState>) -> Router {
    let api = Router::new()
        .route("/v1/info", get(info))
        .route("/v1/query", post(query))
        .route("/v1/agents", get(list_agents))
        .route("/v1/agents/{id}/fs", get(fs_root))
        .route("/v1/agents/{id}/fs/{*path}", get(fs_get).put(fs_put).delete(fs_delete))
        .route("/v1/agents/{id}/mkdir/{*path}", post(fs_mkdir))
        .route("/v1/agents/{id}/kv", get(kv_list))
        .route("/v1/agents/{id}/kv/{key}", get(kv_get).put(kv_put).delete(kv_delete))
        .route("/v1/agents/{id}/tools", get(tools_recent).post(tools_record))
        .route("/v1/agents/{id}/push", post(agent_push))
        .route("/v1/agents/{id}/pull", post(agent_pull))
        .route("/v1/docs", get(docs_list))
        .route("/v1/docs/{doc}", get(doc_get))
        .route("/v1/docs/{doc}/text/{name}", get(text_get).post(text_edit))
        .route("/v1/docs/{doc}/map/{name}", get(map_get).patch(map_patch))
        .route("/v1/docs/{doc}/array/{name}", get(array_get).post(array_push))
        .route("/v1/docs/{doc}/update", get(update_get).post(update_post))
        .route("/v1/docs/{doc}/presence", put(presence_put))
        .route("/v1/memory", post(memory_upsert))
        .route("/v1/memory/search", post(memory_search))
        .route("/v1/memory/{ns}/{id}", axum::routing::delete(memory_delete))
        .route("/v1/transcripts", post(transcripts_append))
        .route("/v1/transcripts/{session}", get(transcripts_get))
        .route("/v1/blob", get(blob_list))
        .route("/v1/blob/{*path}", get(blob_get).put(blob_put))
        .route("/ws/{doc}", get(ws_upgrade))
        .layer(middleware::from_fn_with_state(state.clone(), auth))
        .layer(DefaultBodyLimit::max(256 * 1024 * 1024));

    Router::new()
        .route("/", get(|| async { Html(include_str!("web/index.html")) }))
        .route("/health", get(|| async { "ok" }))
        // DuckDB Quack protocol: `ATTACH 'quack:host:port' AS gw`. Auth is the token in the
        // CONNECT message (DuckDB's quack secret), so this sits outside the bearer middleware.
        .route("/quack", post(quack_post).options(quack_options))
        .route(
            "/static/yjs-bundle.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript; charset=utf-8"), (header::CACHE_CONTROL, "public, max-age=3600")],
                    include_str!("web/yjs-bundle.js"),
                )
            }),
        )
        .merge(api)
        .with_state(state)
}

async fn auth(State(st): St, req: Request, next: Next) -> Response {
    if let Some(token) = &st.cfg.auth_token {
        let bearer = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        // Browsers can't set headers on WebSocket upgrades, so also accept ?token=
        let query_tok = req.uri().query().and_then(|q| {
            url::form_urlencoded::parse(q.as_bytes()).find(|(k, _)| k == "token").map(|(_, v)| v.into_owned())
        });
        let ok = bearer == Some(token.as_str()) || query_tok.as_deref() == Some(token.as_str());
        if !ok {
            return ApiError(StatusCode::UNAUTHORIZED, "missing or invalid token".into()).into_response();
        }
    }
    next.run(req).await
}

async fn info(State(st): St) -> ApiResult<Json<Value>> {
    let tables = run_sql_json(
        &st.ctx,
        "SELECT table_schema, table_name, table_type FROM information_schema.tables \
         WHERE table_schema NOT IN ('information_schema') ORDER BY 1, 2",
        10_000,
        true,
    )
    .await?;
    Ok(Json(json!({
        "name": "agentgate",
        "version": env!("CARGO_PKG_VERSION"),
        "features": {
            "azure": cfg!(feature = "azure"), "postgres": cfg!(feature = "postgres"),
            "snowflake": cfg!(feature = "snowflake"), "duckdb": cfg!(feature = "duckdb"),
            "lancedb": cfg!(feature = "lancedb"),
        },
        "sources": st.sources,
        "tables": tables["rows"],
        "memory_backend": st.memory.backend_name(),
        "transcript_sinks": st.sinks,
        "blob": st.blob.as_ref().map(|b| b.url.clone()),
        "agents": st.agents.list_ids()?,
    })))
}

// ---- SQL --------------------------------------------------------------------------

#[derive(Deserialize)]
struct QueryReq {
    sql: String,
    #[serde(default)]
    format: Option<String>,
}

async fn run_sql_batches(ctx: &SessionContext, sql: &str, allow_writes: bool) -> ApiResult<Vec<RecordBatch>> {
    let opts = SQLOptions::new()
        .with_allow_ddl(allow_writes)
        .with_allow_dml(allow_writes)
        .with_allow_statements(allow_writes);
    let df = ctx.sql_with_options(sql, opts).await?;
    Ok(df.collect().await?)
}

async fn run_sql_json(ctx: &SessionContext, sql: &str, max_rows: usize, allow_writes: bool) -> ApiResult<Value> {
    let batches = run_sql_batches(ctx, sql, allow_writes).await?;
    batches_to_json(&batches, max_rows)
}

fn batches_to_json(batches: &[RecordBatch], max_rows: usize) -> ApiResult<Value> {
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let columns: Vec<Value> = batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| json!({"name": f.name(), "type": f.data_type().to_string()}))
                .collect()
        })
        .unwrap_or_default();
    let mut kept = vec![];
    let mut n = 0;
    for b in batches {
        if n >= max_rows {
            break;
        }
        let take = (max_rows - n).min(b.num_rows());
        kept.push(b.slice(0, take));
        n += take;
    }
    let mut w = datafusion::arrow::json::ArrayWriter::new(Vec::new());
    let refs: Vec<&RecordBatch> = kept.iter().collect();
    w.write_batches(&refs).map_err(|e| ApiError::bad(e.to_string()))?;
    w.finish().map_err(|e| ApiError::bad(e.to_string()))?;
    let buf = w.into_inner();
    let rows: Value = if buf.is_empty() { json!([]) } else { serde_json::from_slice(&buf).unwrap_or(json!([])) };
    Ok(json!({ "columns": columns, "rows": rows, "row_count": total, "truncated": total > n }))
}

async fn query(State(st): St, headers: HeaderMap, Json(req): Json<QueryReq>) -> ApiResult<Response> {
    let started = now_secs();
    let result = run_sql_batches(&st.ctx, &req.sql, st.cfg.allow_sql_writes).await;
    if let Some(agent) = agent_header(&headers) {
        if let Ok(afs) = st.agents.get(&agent).await {
            let outcome = match &result {
                Ok(b) => Ok(json!({"rows": b.iter().map(|b| b.num_rows()).sum::<usize>()})),
                Err(e) => Err(e.1.clone()),
            };
            st.agents.audit(&afs, "sql.query", json!({"sql": req.sql}), started, outcome).await;
        }
    }
    let batches = result?;
    if req.format.as_deref() == Some("arrow") {
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(datafusion::arrow::datatypes::Schema::empty()));
        let mut buf = vec![];
        {
            let mut w = datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema)
                .map_err(|e| ApiError::bad(e.to_string()))?;
            for b in &batches {
                w.write(b).map_err(|e| ApiError::bad(e.to_string()))?;
            }
            w.finish().map_err(|e| ApiError::bad(e.to_string()))?;
        }
        return Ok(([(header::CONTENT_TYPE, "application/vnd.apache.arrow.stream")], buf).into_response());
    }
    Ok(Json(batches_to_json(&batches, st.cfg.max_rows)?).into_response())
}

// ---- agents: filesystem / kv / audit ------------------------------------------------

async fn list_agents(State(st): St) -> ApiResult<Json<Value>> {
    let ids = st.agents.list_ids()?;
    let open = st.agents.open_ids().await;
    Ok(Json(json!({ "agents": ids, "open": open })))
}

async fn fs_root(st: St, Path(id): Path<String>) -> ApiResult<Response> {
    fs_get(st, Path((id, String::new()))).await
}

async fn fs_get(State(st): St, Path((id, path)): Path<(String, String)>) -> ApiResult<Response> {
    let afs = st.agents.get(&id).await?;
    let path = agents::normalize(&path)?;
    let stat = afs.fs.stat(&path).await?.ok_or_else(|| ApiError::not_found(format!("{path} not found")))?;
    if stat.is_directory() {
        let entries = afs.fs.readdir_plus(stat.ino).await?.unwrap_or_default();
        let list: Vec<Value> = entries
            .iter()
            .map(|e| {
                json!({
                    "name": e.name,
                    "type": if e.stats.is_directory() { "dir" } else if e.stats.is_symlink() { "symlink" } else { "file" },
                    "size": e.stats.size, "mtime": e.stats.mtime,
                })
            })
            .collect();
        return Ok(Json(json!({ "path": path, "entries": list })).into_response());
    }
    let data = afs.fs.read_file(&path).await?.unwrap_or_default();
    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], data).into_response())
}

async fn fs_put(
    State(st): St,
    Path((id, path)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let afs = st.agents.get(&id).await?;
    let path = agents::normalize(&path)?;
    let started = now_secs();
    let r = agents::write_file(&afs, &path, &body).await;
    let actor = agent_header(&headers).or_else(|| actor_header(&headers));
    st.agents
        .audit(
            &afs,
            "fs.write",
            json!({"path": path, "bytes": body.len(), "actor": actor}),
            started,
            r.as_ref().map(|_| json!("ok")).map_err(|e| e.to_string()),
        )
        .await;
    r?;
    Ok(Json(json!({ "path": path, "bytes": body.len() })))
}

async fn fs_delete(State(st): St, Path((id, path)): Path<(String, String)>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    let afs = st.agents.get(&id).await?;
    let path = agents::normalize(&path)?;
    let started = now_secs();
    let r = afs.fs.remove(&path).await;
    st.agents
        .audit(
            &afs,
            "fs.remove",
            json!({"path": path, "actor": agent_header(&headers)}),
            started,
            r.as_ref().map(|_| json!("ok")).map_err(|e| e.to_string()),
        )
        .await;
    r?;
    Ok(Json(json!({ "removed": path })))
}

async fn fs_mkdir(State(st): St, Path((id, path)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let afs = st.agents.get(&id).await?;
    let path = agents::normalize(&path)?;
    agents::ensure_parents(&afs, &format!("{path}/x")).await?;
    Ok(Json(json!({ "created": path })))
}

async fn kv_list(State(st): St, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let afs = st.agents.get(&id).await?;
    Ok(Json(json!({ "keys": afs.kv.keys().await? })))
}

async fn kv_get(State(st): St, Path((id, key)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let afs = st.agents.get(&id).await?;
    let v: Option<Value> = afs.kv.get(&key).await?;
    v.map(Json).ok_or_else(|| ApiError::not_found(format!("key {key} not found")))
}

async fn kv_put(
    State(st): St,
    Path((id, key)): Path<(String, String)>,
    headers: HeaderMap,
    Json(v): Json<Value>,
) -> ApiResult<Json<Value>> {
    let afs = st.agents.get(&id).await?;
    let started = now_secs();
    let r = afs.kv.set(&key, &v).await;
    st.agents
        .audit(
            &afs,
            "kv.set",
            json!({"key": key, "actor": agent_header(&headers)}),
            started,
            r.as_ref().map(|_| json!("ok")).map_err(|e| e.to_string()),
        )
        .await;
    r?;
    Ok(Json(json!({ "key": key })))
}

async fn kv_delete(State(st): St, Path((id, key)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let afs = st.agents.get(&id).await?;
    afs.kv.delete(&key).await?;
    Ok(Json(json!({ "deleted": key })))
}

#[derive(Deserialize)]
struct Limit {
    limit: Option<i64>,
}

async fn tools_recent(State(st): St, Path(id): Path<String>, Query(l): Query<Limit>) -> ApiResult<Json<Value>> {
    let afs = st.agents.get(&id).await?;
    let calls = afs.tools.recent(Some(l.limit.unwrap_or(100))).await?;
    Ok(Json(json!({ "tool_calls": calls })))
}

#[derive(Deserialize)]
struct ToolRecord {
    name: String,
    #[serde(default)]
    parameters: Option<Value>,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    started_at: Option<i64>,
    #[serde(default)]
    completed_at: Option<i64>,
}

async fn tools_record(State(st): St, Path(id): Path<String>, Json(t): Json<ToolRecord>) -> ApiResult<Json<Value>> {
    let afs = st.agents.get(&id).await?;
    let now = now_secs();
    let rid = afs
        .tools
        .record(
            &t.name,
            t.started_at.unwrap_or(now),
            t.completed_at.unwrap_or(now),
            t.parameters,
            t.result,
            t.error.as_deref(),
        )
        .await?;
    Ok(Json(json!({ "id": rid })))
}

fn need_blob(st: &AppState) -> ApiResult<Arc<Blob>> {
    st.blob.clone().ok_or_else(|| ApiError::bad("no [blob] url configured"))
}

async fn agent_push(State(st): St, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let blob = need_blob(&st)?;
    let n = blob.push_agent(&st.agents, &id).await?;
    Ok(Json(json!({ "agent": id, "bytes": n, "blob": format!("{}/agents/{id}.db", blob.url) })))
}

async fn agent_pull(State(st): St, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    if !agents::valid_agent_id(&id) {
        return Err(ApiError::bad("invalid agent id"));
    }
    let blob = need_blob(&st)?;
    let n = blob.pull_agent(&st.agents, &id).await?;
    Ok(Json(json!({ "agent": id, "bytes": n })))
}

// ---- CRDT documents ----------------------------------------------------------------

async fn docs_list(State(st): St) -> ApiResult<Json<Value>> {
    let stats = st.store.doc_stats().await?;
    let loaded: HashMap<String, usize> =
        st.hub.loaded_rooms().await.into_iter().map(|r| (r.id.clone(), r.peer_count())).collect();
    Ok(Json(json!({ "persisted": stats, "live": loaded })))
}

async fn doc_get(State(st): St, Path(doc): Path<String>) -> ApiResult<Json<Value>> {
    let room = st.hub.room(&doc).await?;
    let d = room.doc();
    let txn = d.transact();
    let mut roots = serde_json::Map::new();
    for (name, out) in txn.root_refs() {
        let any = out.to_json(&txn);
        let mut s = String::new();
        any.to_json(&mut s);
        roots.insert(name.to_string(), serde_json::from_str(&s).unwrap_or(Value::Null));
    }
    let presence: Vec<Value> = {
        let aw = room.awareness.lock().unwrap();
        aw.iter()
            .filter_map(|(id, state)| {
                state.data.as_ref().map(|d| json!({"client": id.get(), "state": serde_json::from_str::<Value>(d).unwrap_or(Value::Null)}))
            })
            .collect()
    };
    Ok(Json(json!({ "doc": doc, "peers": room.peer_count(), "roots": roots, "presence": presence })))
}

async fn text_get(State(st): St, Path((doc, name)): Path<(String, String)>) -> ApiResult<Response> {
    let room = st.hub.room(&doc).await?;
    let d = room.doc();
    let text = d.get_or_insert_text(name.as_str());
    let s = text.get_string(&d.transact());
    Ok(([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], s).into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum TextOp {
    Append(String),
    Insert { index: u32, text: String },
    Delete { index: u32, len: u32 },
    Replace(String),
}

async fn text_edit(
    State(st): St,
    Path((doc, name)): Path<(String, String)>,
    headers: HeaderMap,
    Json(op): Json<TextOp>,
) -> ApiResult<Json<Value>> {
    let room = st.hub.room(&doc).await?;
    let d = room.doc();
    let text = d.get_or_insert_text(name.as_str());
    let origin = http_origin(&headers);
    let (op_name, len) = {
        let mut txn = d.transact_mut_with(origin.clone());
        let len = text.len(&txn);
        let op_name = match &op {
            TextOp::Append(s) => {
                text.push(&mut txn, s);
                "append"
            }
            TextOp::Insert { index, text: s } => {
                text.insert(&mut txn, (*index).min(len), s);
                "insert"
            }
            TextOp::Delete { index, len: n } => {
                let i = (*index).min(len);
                text.remove_range(&mut txn, i, (*n).min(len - i));
                "delete"
            }
            TextOp::Replace(s) => {
                text.remove_range(&mut txn, 0, len);
                text.insert(&mut txn, 0, s);
                "replace"
            }
        };
        (op_name, text.len(&txn))
    };
    audit_doc(&st, &headers, &format!("doc.text.{op_name}"), json!({"doc": doc, "text": name})).await;
    Ok(Json(json!({ "doc": doc, "text": name, "len": len })))
}

async fn map_get(State(st): St, Path((doc, name)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let room = st.hub.room(&doc).await?;
    let d = room.doc();
    let map = d.get_or_insert_map(name.as_str());
    let txn = d.transact();
    let mut s = String::new();
    map.to_json(&txn).to_json(&mut s);
    Ok(Json(serde_json::from_str(&s).unwrap_or(Value::Null)))
}

async fn map_patch(
    State(st): St,
    Path((doc, name)): Path<(String, String)>,
    headers: HeaderMap,
    Json(patch): Json<serde_json::Map<String, Value>>,
) -> ApiResult<Json<Value>> {
    let room = st.hub.room(&doc).await?;
    let d = room.doc();
    let map = d.get_or_insert_map(name.as_str());
    {
        let mut txn = d.transact_mut_with(http_origin(&headers));
        for (k, v) in &patch {
            if v.is_null() {
                map.remove(&mut txn, k);
            } else {
                let any = yrs::Any::from_json(&v.to_string()).map_err(|e| ApiError::bad(e.to_string()))?;
                map.insert(&mut txn, k.as_str(), any);
            }
        }
    }
    audit_doc(&st, &headers, "doc.map.patch", json!({"doc": doc, "map": name, "keys": patch.keys().collect::<Vec<_>>()}))
        .await;
    Ok(Json(json!({ "doc": doc, "map": name, "updated": patch.len() })))
}

async fn array_get(State(st): St, Path((doc, name)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let room = st.hub.room(&doc).await?;
    let d = room.doc();
    let arr = d.get_or_insert_array(name.as_str());
    let txn = d.transact();
    let mut s = String::new();
    arr.to_json(&txn).to_json(&mut s);
    Ok(Json(serde_json::from_str(&s).unwrap_or(Value::Null)))
}

#[derive(Deserialize)]
struct ArrayPush {
    push: Value,
}

async fn array_push(
    State(st): St,
    Path((doc, name)): Path<(String, String)>,
    headers: HeaderMap,
    Json(p): Json<ArrayPush>,
) -> ApiResult<Json<Value>> {
    let room = st.hub.room(&doc).await?;
    let d = room.doc();
    let arr = d.get_or_insert_array(name.as_str());
    let any = yrs::Any::from_json(&p.push.to_string()).map_err(|e| ApiError::bad(e.to_string()))?;
    let len = {
        let mut txn = d.transact_mut_with(http_origin(&headers));
        arr.push_back(&mut txn, any);
        arr.len(&txn)
    };
    audit_doc(&st, &headers, "doc.array.push", json!({"doc": doc, "array": name})).await;
    Ok(Json(json!({ "doc": doc, "array": name, "len": len })))
}

#[derive(Deserialize)]
struct SvQuery {
    /// base64 state vector; omit for the full document state.
    sv: Option<String>,
}

async fn update_get(State(st): St, Path(doc): Path<String>, Query(q): Query<SvQuery>) -> ApiResult<Response> {
    let room = st.hub.room(&doc).await?;
    let bytes = match q.sv {
        Some(sv) => {
            let sv = base64::engine::general_purpose::STANDARD
                .decode(sv)
                .map_err(|e| ApiError::bad(format!("sv: {e}")))?;
            room.diff(&sv)?
        }
        None => room.full_state(),
    };
    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response())
}

async fn update_post(State(st): St, Path(doc): Path<String>, headers: HeaderMap, body: Bytes) -> ApiResult<Json<Value>> {
    let room = st.hub.room(&doc).await?;
    room.apply_update(&http_origin(&headers), &body)?;
    audit_doc(&st, &headers, "doc.update", json!({"doc": doc, "bytes": body.len()})).await;
    Ok(Json(json!({ "doc": doc, "applied_bytes": body.len() })))
}

#[derive(Deserialize)]
struct Presence {
    #[serde(default)]
    state: Option<Value>,
}

async fn presence_put(
    State(st): St,
    Path(doc): Path<String>,
    headers: HeaderMap,
    Json(p): Json<Presence>,
) -> ApiResult<Json<Value>> {
    let who = agent_header(&headers)
        .map(|a| format!("agent:{a}"))
        .or_else(|| actor_header(&headers).map(|u| format!("human:{u}")))
        .ok_or_else(|| ApiError::bad("X-Agent-Id or X-Actor header required"))?;
    let room = st.hub.room(&doc).await?;
    // Stable synthetic awareness client id per HTTP participant (high range to avoid Yjs ids).
    let client_id = (fxhash(&who) & 0x1F_FFFF_FFFF_FFFF) | 0x10_0000_0000_0000;
    room.set_presence(&who, client_id, p.state)?;
    Ok(Json(json!({ "doc": doc, "client_id": client_id })))
}

fn fxhash(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

async fn audit_doc(st: &AppState, headers: &HeaderMap, name: &str, params: Value) {
    if let Some(agent) = agent_header(headers) {
        if let Ok(afs) = st.agents.get(&agent).await {
            st.agents.audit(&afs, name, params, now_secs(), Ok(json!("ok"))).await;
        }
    }
}

// ---- WebSocket (y-websocket compatible) ---------------------------------------------

#[derive(Deserialize)]
struct WsParams {
    agent: Option<String>,
    user: Option<String>,
}

async fn ws_upgrade(
    State(st): St,
    Path(doc): Path<String>,
    Query(p): Query<WsParams>,
    ws: WebSocketUpgrade,
) -> ApiResult<Response> {
    let room = st.hub.room(&doc).await?;
    let n = st.peer_seq.fetch_add(1, Ordering::Relaxed);
    let origin = match (p.agent, p.user) {
        (Some(a), _) => format!("agent:{a}#{n}"),
        (None, Some(u)) => format!("human:{u}#{n}"),
        _ => format!("ws#{n}"),
    };
    Ok(ws.max_message_size(64 * 1024 * 1024).on_upgrade(move |socket| peer(socket, room, origin)))
}

async fn peer(socket: WebSocket, room: Arc<crate::crdt::Room>, origin: String) {
    tracing::info!(doc = %room.id, peer = %origin, "peer connected");
    let (mut tx, mut rx) = socket.split();
    let mut bus = room.tx.subscribe();
    let mut my_clients: Vec<yrs::ClientID> = vec![];

    match room.handshake() {
        Ok(msgs) => {
            for m in msgs {
                if tx.send(WsMessage::Binary(m.into())).await.is_err() {
                    return;
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "handshake failed");
            return;
        }
    }

    loop {
        tokio::select! {
            incoming = rx.next() => {
                match incoming {
                    Some(Ok(WsMessage::Binary(data))) => match room.handle(&origin, &data) {
                        Ok(h) => {
                            for c in h.awareness_clients {
                                if !my_clients.contains(&c) { my_clients.push(c); }
                            }
                            for r in h.replies {
                                if tx.send(WsMessage::Binary(r.into())).await.is_err() { break; }
                            }
                        }
                        Err(e) => tracing::warn!(peer = %origin, error = %e, "bad frame"),
                    },
                    Some(Ok(WsMessage::Ping(p))) => { let _ = tx.send(WsMessage::Pong(p)).await; }
                    Some(Ok(WsMessage::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(_)) => {}
                }
            }
            frame = bus.recv() => {
                match frame {
                    Ok(Frame { origin: o, bytes }) => {
                        if *o != *origin
                            && tx.send(WsMessage::Binary(bytes.as_ref().clone().into())).await.is_err()
                        {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Slow peer: resynchronise with a full handshake instead of dropping updates.
                        if let Ok(msgs) = room.handshake() {
                            for m in msgs {
                                if tx.send(WsMessage::Binary(m.into())).await.is_err() { break; }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    room.remove_presence(&my_clients);
    tracing::info!(doc = %room.id, peer = %origin, "peer disconnected");
}

// ---- memory -------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    Many(Vec<T>),
    One(T),
}

async fn memory_upsert(
    State(st): St,
    headers: HeaderMap,
    Json(body): Json<OneOrMany<MemoryRecord>>,
) -> ApiResult<Json<Value>> {
    let recs = match body {
        OneOrMany::One(r) => vec![r],
        OneOrMany::Many(v) => v,
    };
    let agent = agent_header(&headers);
    for mut r in recs.clone() {
        if r.agent_id.is_none() {
            r.agent_id = agent.clone();
        }
        st.memory.upsert(&r).await?;
    }
    audit_doc(&st, &headers, "memory.upsert", json!({"count": recs.len()})).await;
    Ok(Json(json!({ "upserted": recs.len(), "backend": st.memory.backend_name() })))
}

async fn memory_search(State(st): St, Json(q): Json<MemoryQuery>) -> ApiResult<Json<Value>> {
    let hits = st.memory.search(&q).await?;
    Ok(Json(json!({ "hits": hits })))
}

async fn memory_delete(State(st): St, Path((ns, id)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let deleted = st.memory.delete(&ns, &id).await?;
    Ok(Json(json!({ "deleted": deleted })))
}

// ---- transcripts --------------------------------------------------------------------

async fn transcripts_append(
    State(st): St,
    headers: HeaderMap,
    Json(body): Json<OneOrMany<TranscriptEntry>>,
) -> ApiResult<Json<Value>> {
    let entries = match body {
        OneOrMany::One(e) => vec![e],
        OneOrMany::Many(v) => v,
    };
    let default_actor = agent_header(&headers)
        .map(|a| format!("agent:{a}"))
        .or_else(|| actor_header(&headers).map(|u| format!("human:{u}")));
    let mut seqs = vec![];
    for mut e in entries {
        if e.session_id.is_empty() {
            return Err(ApiError::bad("session_id required"));
        }
        if e.actor.is_none() {
            e.actor = default_actor.clone();
        }
        seqs.push(st.store.transcript_append(&e).await?);
    }
    Ok(Json(json!({ "seqs": seqs })))
}

#[derive(Deserialize)]
struct After {
    after: Option<i64>,
    limit: Option<i64>,
}

async fn transcripts_get(State(st): St, Path(session): Path<String>, Query(a): Query<After>) -> ApiResult<Json<Value>> {
    let rows = st
        .store
        .transcripts_after(a.after.unwrap_or(0), a.limit.unwrap_or(1000).clamp(1, 10_000), Some(&session))
        .await?;
    Ok(Json(json!({ "session_id": session, "entries": rows })))
}

// ---- blob artifacts ------------------------------------------------------------------

fn artifact_key(path: &str) -> ApiResult<String> {
    let p = agents::normalize(path)?;
    Ok(format!("artifacts{p}"))
}

async fn blob_put(State(st): St, Path(path): Path<String>, headers: HeaderMap, body: Bytes) -> ApiResult<Json<Value>> {
    let blob = need_blob(&st)?;
    let key = artifact_key(&path)?;
    let n = body.len();
    blob.put(&key, body).await?;
    audit_doc(&st, &headers, "blob.put", json!({"key": key, "bytes": n})).await;
    Ok(Json(json!({ "key": key, "bytes": n, "url": format!("{}/{key}", blob.url) })))
}

async fn blob_get(State(st): St, Path(path): Path<String>) -> ApiResult<Response> {
    let blob = need_blob(&st)?;
    let key = artifact_key(&path)?;
    let data = blob.get(&key).await?.ok_or_else(|| ApiError::not_found(format!("{key} not found")))?;
    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], data).into_response())
}

async fn blob_list(State(st): St) -> ApiResult<Json<Value>> {
    let blob = need_blob(&st)?;
    let items = blob.list("").await?;
    let list: Vec<Value> = items.into_iter().map(|(k, s)| json!({"key": k, "size": s})).collect();
    Ok(Json(json!({ "url": blob.url, "objects": list })))
}

// ---- Quack ------------------------------------------------------------------------

async fn quack_post(State(st): St, body: Bytes) -> Response {
    let resp = st.quack.handle(&body).await;
    (
        [(header::CONTENT_TYPE, "application/vnd.duckdb"), (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")],
        resp,
    )
        .into_response()
}

async fn quack_options() -> Response {
    (
        StatusCode::NO_CONTENT,
        [
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
            (header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS"),
            (header::ACCESS_CONTROL_ALLOW_HEADERS, "*"),
        ],
    )
        .into_response()
}
