//! Quack (DuckDB client-server protocol, v3) endpoint.
//!
//! DuckDB clients attach enact-db like any Quack server:
//!
//! ```sql
//! CREATE SECRET (TYPE quack, TOKEN 'secret');       -- if a token is configured
//! ATTACH 'quack:localhost:9494' AS gw;
//! FROM gw.customers;                                -- federated source (Postgres, Databricks, ...)
//! FROM gw.system.agent_tool_calls;                  -- live agent audit
//! FROM quack_query('gw', 'SELECT ... any DataFusion SQL ...');
//! ```
//!
//! What the server does with each request:
//! * CONNECT: checks the token against `[quack] token` (or the server token)
//! * the two catalog queries a client sends at ATTACH (`duckdb_schemas()` /
//!   `duckdb_tables()`) are answered from enact-db's catalog: `public` is exposed
//!   as DuckDB's `main`, plus `system`
//! * scans (`SELECT #1, #3 FROM main.t`) are rewritten to DataFusion SQL;
//!   anything else (`quack_query`) runs on DataFusion as-is
//! * results stream: rows are inlined in PREPARE up to `inline_rows`, the
//!   remainder is produced lazily per FETCH batch (retries and fetch-ahead
//!   served from a per-query cache until acknowledged)
//! * writes (SEND_DATA / INSERT) are rejected: the endpoint is read-only

pub mod vector;
pub mod wire;

use anyhow::{anyhow, Result};
use datafusion::arrow::array::{ArrayRef, Int64Array, ListBuilder, StringArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use vector::{duck_types, encode_chunks, DuckType};
use wire::{msg, Reader, Request, Uuid, Writer};

/// Rows inlined in a PREPARE response when the client does not say (DuckDB's default).
const DEFAULT_INLINE_ROWS: u64 = 24_576;
/// Target rows per FETCH batch.
const BATCH_ROWS: usize = 16 * vector::VECTOR_SIZE;
/// Drop idle connections after this long without any request.
const IDLE_TIMEOUT: Duration = Duration::from_secs(3600);

const CATALOG: &str = "gateway";

pub struct QuackServer {
    ctx: SessionContext,
    token: Option<String>,
    conns: Mutex<HashMap<String, Arc<Mutex<Conn>>>>,
}

struct Conn {
    last_seen: Instant,
    query: Option<QueryState>,
}

struct QueryState {
    uuid: Uuid,
    types: Vec<DuckType>,
    stream: Option<SendableRecordBatchStream>,
    /// Encoded chunks (object bytes, rows) not yet sent.
    pending: VecDeque<(Vec<u8>, usize)>,
    /// FETCH batch index -> full response bytes, kept until acknowledged.
    served: BTreeMap<u64, Vec<u8>>,
    /// Next FETCH batch index to produce (dense, from 1).
    next_index: u64,
    finished: bool,
}

impl QueryState {
    /// Pull from the stream until at least `rows` rows are pending or it ends.
    async fn fill(&mut self, rows: usize) -> Result<()> {
        while !self.finished && self.pending.iter().map(|c| c.1).sum::<usize>() < rows {
            match self.stream.as_mut() {
                None => self.finished = true,
                Some(s) => match s.next().await {
                    Some(batch) => {
                        let batch = batch?;
                        if batch.num_rows() > 0 {
                            self.pending.extend(encode_chunks(&batch, &self.types)?);
                        }
                    }
                    None => {
                        self.finished = true;
                        self.stream = None;
                    }
                },
            }
        }
        Ok(())
    }

    /// Take whole chunks while fewer than `rows` rows are taken (DuckDB's
    /// server does the same: it may overshoot by up to one chunk).
    fn take(&mut self, rows: usize) -> Vec<(Vec<u8>, usize)> {
        let mut out = vec![];
        let mut taken = 0;
        while taken < rows {
            let Some(c) = self.pending.pop_front() else { break };
            taken += c.1;
            out.push(c);
        }
        out
    }

    fn exhausted(&self) -> bool {
        self.finished && self.pending.is_empty()
    }
}

/// How DuckDB names an exception type, used so the client rethrows the right kind.
fn error_kind(e: &anyhow::Error) -> &'static str {
    let s = e.to_string();
    if s.contains("not found") || s.contains("No table named") || s.contains("does not exist") {
        "Catalog"
    } else if s.contains("SQL error") || s.contains("ParserError") {
        "Parser"
    } else if s.contains("Schema error") || s.contains("Error during planning") {
        "Binder"
    } else if s.contains("read-only") {
        "Permission"
    } else {
        "Invalid Input"
    }
}

impl QuackServer {
    pub fn new(ctx: SessionContext, token: Option<String>) -> Self {
        Self { ctx, token, conns: Mutex::new(HashMap::new()) }
    }

    /// Handle one `POST /quack` body; always returns a Quack message.
    pub async fn handle(&self, body: &[u8]) -> Vec<u8> {
        match self.dispatch(body).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::debug!(error = %e, "quack request failed");
                wire::error_response(&format!("{e:#}"), error_kind(&e))
            }
        }
    }

    async fn dispatch(&self, body: &[u8]) -> Result<Vec<u8>> {
        let mut r = Reader::new(body);
        let header = wire::read_header(&mut r)?;
        let req = wire::read_request(&header, &mut r)?;
        if let Request::Connect { auth, client_version, min_version, max_version, heartbeat } = req {
            if min_version > wire::QUACK_VERSION || max_version < wire::QUACK_VERSION {
                return Err(anyhow!(
                    "Unsupported Quack version - server only supports version {} of quack",
                    wire::QUACK_VERSION
                ));
            }
            if let Some(t) = &self.token {
                if !constant_time_eq(auth.as_bytes(), t.as_bytes()) {
                    return Err(anyhow!("Authentication failed"));
                }
            }
            let id = new_connection_id();
            let mut conns = self.conns.lock().await;
            let now = Instant::now();
            let mut stale = vec![];
            for (k, c) in conns.iter() {
                if let Ok(c) = c.try_lock() {
                    if now.duration_since(c.last_seen) > IDLE_TIMEOUT {
                        stale.push(k.clone());
                    }
                }
            }
            for k in stale {
                conns.remove(&k);
            }
            conns.insert(id.clone(), Arc::new(Mutex::new(Conn { last_seen: now, query: None })));
            tracing::info!(connection = %id, client = %client_version, "quack client connected");
            return Ok(wire::connection_response(&id, heartbeat.max(1)));
        }

        let conn = self
            .conns
            .lock()
            .await
            .get(&header.connection_id)
            .cloned()
            .ok_or_else(|| anyhow!("Invalid connection id"))?;
        let mut conn = conn.lock().await;
        conn.last_seen = Instant::now();

        match req {
            Request::Connect { .. } => unreachable!(),
            Request::Disconnect => {
                drop(conn);
                self.conns.lock().await.remove(&header.connection_id);
                Ok(wire::success_response())
            }
            Request::Heartbeat | Request::Ack { .. } => Ok(wire::success_response()),
            Request::Cancel { uuid } => {
                if conn.query.as_ref().map(|q| q.uuid) == Some(uuid) {
                    conn.query = None;
                }
                Ok(wire::success_response())
            }
            Request::SendData => Err(anyhow!("enact-db's Quack endpoint is read-only")),
            Request::Prepare { sql, uuid, inline_rows } => {
                conn.query = None; // a new query supersedes the previous one
                let (schema, stream) = self.run(&sql).await?;
                let types = duck_types(&schema);
                let mut q = QueryState {
                    uuid,
                    types,
                    stream,
                    pending: VecDeque::new(),
                    served: BTreeMap::new(),
                    next_index: 1,
                    finished: false,
                };
                let max_inline = inline_rows.unwrap_or(DEFAULT_INLINE_ROWS) as usize;
                let inline = if max_inline > 0 {
                    q.fill(max_inline).await?;
                    q.take(max_inline)
                } else {
                    vec![]
                };
                if max_inline > 0 {
                    // know whether anything is left without blocking on a whole extra batch
                    q.fill(1).await?;
                }
                let needs_more_fetch = !q.exhausted();
                let resp = prepare_response(&schema, &q.types, &inline, needs_more_fetch, uuid);
                if needs_more_fetch {
                    conn.query = Some(q);
                }
                Ok(resp)
            }
            Request::Fetch { uuid, batch_index, ack_index } => {
                let q = match conn.query.as_mut() {
                    Some(q) if q.uuid == uuid => q,
                    _ => return Err(anyhow!("Result has been closed")),
                };
                if batch_index == 0 {
                    return Err(anyhow!("FETCH_REQUEST is missing its batch index"));
                }
                q.served.retain(|k, _| *k > ack_index);
                if let Some(b) = q.served.get(&batch_index) {
                    return Ok(b.clone());
                }
                // produce batches up to the requested index (fetch-ahead may ask out of order)
                while q.next_index <= batch_index {
                    q.fill(BATCH_ROWS).await?;
                    if q.pending.is_empty() {
                        break;
                    }
                    let chunks = q.take(BATCH_ROWS);
                    let idx = q.next_index;
                    q.served.insert(idx, fetch_response(&chunks, Some(idx), None));
                    q.next_index += 1;
                }
                if let Some(b) = q.served.get(&batch_index) {
                    return Ok(b.clone());
                }
                // past the end: terminal response with the total batch count
                Ok(fetch_response(&[], None, Some(q.next_index - 1)))
            }
        }
    }

    /// Execute SQL from a client: catalog emulation, scan translation, or passthrough.
    async fn run(&self, sql: &str) -> Result<(SchemaRef, Option<SendableRecordBatchStream>)> {
        let lower = sql.to_ascii_lowercase();
        if lower.contains("from duckdb_schemas()") {
            let b = self.schema_rows()?;
            return Ok((b.schema(), Some(one_batch_stream(b))));
        }
        if lower.contains("from duckdb_tables()") {
            let b = self.table_rows().await?;
            return Ok((b.schema(), Some(one_batch_stream(b))));
        }
        let translated = self.translate(sql).await;
        tracing::debug!(sql, translated = %translated, "quack query");
        let opts = datafusion::execution::context::SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = self.ctx.sql_with_options(&translated, opts).await.map_err(|e| {
            let msg = e.to_string();
            if msg.contains("DDL") || msg.contains("DML") || msg.contains("Statement not supported") {
                anyhow!("enact-db's Quack endpoint is read-only: {msg}")
            } else {
                anyhow!(msg)
            }
        })?;
        let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
        let stream = df.execute_stream().await?;
        Ok((schema, Some(stream)))
    }

    /// Every schema exposed to DuckDB clients, in a stable order: the gateway's
    /// `public` (as `main`) and `system`, then each mounted catalog (e.g. an
    /// Iceberg catalog `lake`) as nested schemas `lake.<namespace>`.
    pub fn quack_schemas(&self) -> Vec<QSchema> {
        let mut out = vec![
            QSchema { oid: 1, path: vec!["main".into()], catalog: CATALOG.into(), schema: "public".into() },
            QSchema { oid: 2, path: vec!["system".into()], catalog: CATALOG.into(), schema: "system".into() },
        ];
        let mut catalogs = self.ctx.catalog_names();
        catalogs.sort();
        for c in catalogs.into_iter().filter(|c| c != CATALOG) {
            let Some(cat) = self.ctx.catalog(&c) else { continue };
            let mut schemas = cat.schema_names();
            schemas.sort();
            for s in schemas.into_iter().filter(|s| s != "information_schema") {
                let oid = out.len() as i64 + 1;
                out.push(QSchema { oid, path: vec![c.clone(), s.clone()], catalog: c.clone(), schema: s });
            }
        }
        out
    }

    fn schema_rows(&self) -> Result<RecordBatch> {
        let schemas = self.quack_schemas();
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("database_name", DataType::Utf8, false),
            Field::new("schema_path", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("current_database()", DataType::Utf8, false),
        ]));
        let mut paths = ListBuilder::new(StringBuilder::new());
        for q in &schemas {
            for part in &q.path {
                paths.values().append_value(part);
            }
            paths.append(true);
        }
        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(schemas.iter().map(|s| s.oid).collect::<Vec<_>>())) as ArrayRef,
                Arc::new(StringArray::from(vec![CATALOG; schemas.len()])),
                Arc::new(paths.finish()),
                Arc::new(StringArray::from(vec![CATALOG; schemas.len()])),
            ],
        )?)
    }

    async fn table_rows(&self) -> Result<RecordBatch> {
        let (mut oids, mut sqls) = (vec![], vec![]);
        for q in self.quack_schemas() {
            let Some(s) = self.ctx.catalog(&q.catalog).and_then(|c| c.schema(&q.schema)) else { continue };
            let mut names = s.table_names();
            names.sort();
            for name in names {
                match s.table(&name).await {
                    Ok(Some(t)) => {
                        oids.push(q.oid);
                        sqls.push(vector::create_table_sql(&name, &t.schema()));
                    }
                    Ok(None) => {}
                    Err(e) => tracing::warn!(table = %name, error = %e, "skipping table in quack catalog"),
                }
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("schema_oid", DataType::Int64, false),
            Field::new("sql", DataType::Utf8, false),
            Field::new("'table'", DataType::Utf8, false),
        ]));
        let n = oids.len();
        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(oids)) as ArrayRef,
                Arc::new(StringArray::from(sqls)),
                Arc::new(StringArray::from(vec!["table"; n])),
            ],
        )?)
    }

    /// Rewrite the client's scan SQL (`SELECT #1, NULL::BOOLEAN FROM main.t` /
    /// `FROM main.t`) into DataFusion SQL; other SQL passes through (with
    /// DuckDB's FROM-first shorthand expanded).
    async fn translate(&self, sql: &str) -> String {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        if let Some((select, from)) = split_scan(trimmed) {
            if let Some((q, table, schema)) = self.resolve(from).await {
                let target = format!(
                    "{}.{}.{}",
                    vector::quote_ident(&q.catalog),
                    vector::quote_ident(&q.schema),
                    vector::quote_ident(&table)
                );
                let list = match select {
                    None => "*".to_string(),
                    Some(items) => items
                        .split(',')
                        .map(|item| {
                            let item = item.trim();
                            match item.strip_prefix('#').and_then(|n| n.parse::<usize>().ok()) {
                                Some(n) if n >= 1 && n <= schema.fields().len() => {
                                    vector::quote_ident(schema.field(n - 1).name())
                                }
                                _ => item.to_string(),
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", "),
                };
                return format!("SELECT {list} FROM {target}");
            }
        }
        if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("from ") {
            return format!("SELECT * {trimmed}");
        }
        trimmed.to_string()
    }

    /// Resolve a DuckDB-side qualified name (`t`, `main.t`, `lake.sales.t`,
    /// optionally prefixed by the catalog name) to (schema, table, arrow schema).
    async fn resolve(&self, name: &str) -> Option<(QSchema, String, SchemaRef)> {
        let mut parts = split_ident_path(name)?;
        if parts.len() == 1 {
            parts.insert(0, "main".into());
        }
        let table = parts.pop()?;
        let schemas = self.quack_schemas();
        let eq = |a: &[String], b: &[String]| a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y));
        let q = schemas
            .into_iter()
            .find(|q| eq(&parts, &q.path) || (parts.len() == q.path.len() + 1 && parts[0].eq_ignore_ascii_case(CATALOG) && eq(&parts[1..], &q.path)))?;
        let t = self.ctx.catalog(&q.catalog)?.schema(&q.schema)?.table(&table).await.ok()??;
        Some((q, table, t.schema()))
    }
}

#[derive(Debug, Clone)]
pub struct QSchema {
    pub oid: i64,
    /// Schema path as DuckDB sees it (["main"], ["lake", "sales"]).
    pub path: Vec<String>,
    pub catalog: String,
    pub schema: String,
}

/// `SELECT <list> FROM <name>` / `FROM <name>` where name is a plain identifier path.
fn split_scan(sql: &str) -> Option<(Option<&str>, &str)> {
    let lower = sql.to_ascii_lowercase();
    let (select, rest) = if lower.starts_with("select ") {
        let idx = find_keyword(&lower, " from ")?;
        (Some(sql[7..idx].trim()), sql[idx + 6..].trim())
    } else if lower.starts_with("from ") {
        (None, sql[5..].trim())
    } else {
        return None;
    };
    // only a bare (optionally quoted) table reference qualifies as a scan
    split_ident_path(rest).map(|_| (select, rest))
}

/// Byte index of a keyword outside double quotes.
fn find_keyword(lower: &str, kw: &str) -> Option<usize> {
    let mut in_quotes = false;
    let bytes = lower.as_bytes();
    for i in 0..bytes.len() {
        if bytes[i] == b'"' {
            in_quotes = !in_quotes;
        }
        if !in_quotes && lower[i..].starts_with(kw) {
            return Some(i);
        }
    }
    None
}

/// `"a"."b c".d` → ["a", "b c", "d"]; None if not a plain identifier path.
fn split_ident_path(s: &str) -> Option<Vec<String>> {
    let mut parts = vec![];
    let mut cur = String::new();
    let mut chars = s.chars().peekable();
    let mut quoted = false;
    while let Some(c) = chars.next() {
        if quoted {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    quoted = false;
                }
            } else {
                cur.push(c);
            }
        } else if c == '"' {
            quoted = true;
        } else if c == '.' {
            parts.push(std::mem::take(&mut cur));
        } else if c.is_alphanumeric() || c == '_' || c == '$' {
            cur.push(c);
        } else {
            return None;
        }
    }
    if quoted {
        return None;
    }
    parts.push(cur);
    parts.iter().all(|p| !p.is_empty()).then_some(parts)
}

fn one_batch_stream(batch: RecordBatch) -> SendableRecordBatchStream {
    let schema = batch.schema();
    Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        schema,
        futures::stream::once(async move { Ok(batch) }),
    ))
}

fn prepare_response(
    schema: &Schema,
    types: &[DuckType],
    chunks: &[(Vec<u8>, usize)],
    needs_more_fetch: bool,
    uuid: Uuid,
) -> Vec<u8> {
    let mut w = Writer::new();
    wire::write_header(&mut w, msg::PREPARE_RESPONSE, "", None);
    if !types.is_empty() {
        w.field(1).uvar(types.len() as u64);
        for t in types {
            t.write(&mut w);
        }
        w.field(2).uvar(schema.fields().len() as u64);
        for f in schema.fields() {
            w.string(f.name());
        }
    }
    w.prop_bool_default(3, needs_more_fetch);
    if !chunks.is_empty() {
        // vector<unique_ptr<DataChunkWrapper>>: present-byte + {300: {chunk}}
        w.field(4).uvar(chunks.len() as u64);
        for (chunk, _) in chunks {
            w.boolean(true);
            w.field(300);
            w.buf.extend_from_slice(chunk);
            w.end();
        }
    }
    w.field(5).hugeint(uuid.0, uuid.1);
    w.end();
    w.buf
}

/// FETCH_RESPONSE: header, body {chunk_count, total_batches?, batch_index?}, then the raw chunk blob.
fn fetch_response(chunks: &[(Vec<u8>, usize)], batch_index: Option<u64>, total_batches: Option<u64>) -> Vec<u8> {
    let mut w = Writer::new();
    wire::write_header(&mut w, msg::FETCH_RESPONSE, "", None);
    w.prop_uvar_default(1, chunks.len() as u64, 0);
    w.prop_optional_idx_default(2, total_batches);
    w.prop_optional_idx_default(3, batch_index);
    w.end();
    for (chunk, _) in chunks {
        w.buf.extend_from_slice(chunk);
    }
    w.buf
}

fn new_connection_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos();
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let mut x = (nanos as u64) ^ seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (std::process::id() as u64) << 32;
    let mut out = String::with_capacity(32);
    for _ in 0..2 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.push_str(&format!("{x:016x}"));
    }
    out
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_shapes() {
        assert_eq!(split_scan("SELECT #1, #3 FROM main.customers"), Some((Some("#1, #3"), "main.customers")));
        assert_eq!(split_scan("FROM \"system\".\"agent_kv\""), Some((None, "\"system\".\"agent_kv\"")));
        assert_eq!(split_scan("SELECT NULL::BOOLEAN FROM main.t"), Some((Some("NULL::BOOLEAN"), "main.t")));
        assert_eq!(split_scan("SELECT a FROM t JOIN u ON t.x = u.x"), None);
        assert_eq!(split_ident_path("\"we\"\"ird\".t").unwrap(), vec!["we\"ird", "t"]);
    }
}
