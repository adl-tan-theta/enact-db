//! Databricks source against a mock that follows the SQL Statement Execution
//! API contract (field names/paths from the official Databricks SDK):
//! PENDING -> SUCCEEDED polling, ARROW_STREAM + EXTERNAL_LINKS, multiple chunks
//! via next_chunk_internal_link, presigned links that must NOT get the token
//! but MUST get the returned http_headers.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use datafusion::arrow::array::{ArrayRef, Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::{json, Value};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Mock {
    statements: Vec<String>,
    polls: usize,
    leaked_token: bool,
    missing_storage_header: bool,
    api_auth_ok: bool,
}

type M = Arc<Mutex<Mock>>;

fn full_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Decimal128(10, 2), true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("Etc/UTC".into())), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["EU", "US", "EU", "APAC"])),
            Arc::new(Decimal128Array::from(vec![10050, 20000, 2525, 999]).with_precision_and_scale(10, 2).unwrap()),
            Arc::new(TimestampMicrosecondArray::from(vec![0, 1, 2, 3]).with_timezone("Etc/UTC")),
        ],
    )
    .unwrap()
}

/// Columns of the SELECT list (mock "execution": projection only; filters are
/// re-applied by DataFusion because pushdown is Inexact).
fn project(sql: &str) -> RecordBatch {
    let full = full_batch();
    let sel = sql.trim_start_matches("SELECT ").split(" FROM ").next().unwrap();
    if sel == "*" {
        return full;
    }
    if sel.starts_with("1 AS") {
        let s = Arc::new(Schema::new(vec![Field::new("__agentgate_rowcount", DataType::Int64, false)]));
        return RecordBatch::try_new(s, vec![Arc::new(Int64Array::from(vec![1, 1, 1, 1]))]).unwrap();
    }
    let idx: Vec<usize> = sel
        .split(", ")
        .map(|c| full.schema().index_of(c.trim_matches('`')).unwrap())
        .collect();
    full.project(&idx).unwrap()
}

fn ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = vec![];
    let mut w = datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
    w.write(batch).unwrap();
    w.finish().unwrap();
    drop(w);
    buf
}

fn manifest() -> Value {
    json!({"format": "ARROW_STREAM", "schema": {"column_count": 4, "columns": [
        {"name": "id", "type_name": "LONG", "type_text": "BIGINT", "position": 0},
        {"name": "region", "type_name": "STRING", "type_text": "STRING", "position": 1},
        {"name": "amount", "type_name": "DECIMAL", "type_text": "DECIMAL(10,2)", "type_precision": 10, "type_scale": 2, "position": 2},
        {"name": "ts", "type_name": "TIMESTAMP", "type_text": "TIMESTAMP", "position": 3}
    ]}, "total_chunk_count": 2})
}

fn check_auth(m: &M, h: &HeaderMap) -> bool {
    let ok = h.get("authorization").and_then(|v| v.to_str().ok()) == Some("Bearer dapi-test-token");
    m.lock().unwrap().api_auth_ok = ok;
    ok
}

async fn submit(State(m): State<M>, h: HeaderMap, Json(body): Json<Value>) -> impl IntoResponse {
    if !check_auth(&m, &h) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"message": "bad token"})));
    }
    assert_eq!(body["format"], "ARROW_STREAM");
    assert_eq!(body["disposition"], "EXTERNAL_LINKS");
    assert_eq!(body["warehouse_id"], "wh-123");
    let mut g = m.lock().unwrap();
    g.statements.push(body["statement"].as_str().unwrap().to_string());
    let id = g.statements.len() - 1;
    (StatusCode::OK, Json(json!({"statement_id": format!("st-{id}"), "status": {"state": "PENDING"}})))
}

async fn poll(State(m): State<M>, h: HeaderMap, Path(id): Path<String>) -> impl IntoResponse {
    if !check_auth(&m, &h) {
        return (StatusCode::UNAUTHORIZED, Json(json!({})));
    }
    let n: usize = id.trim_start_matches("st-").parse().unwrap();
    let mut g = m.lock().unwrap();
    g.polls += 1;
    let sql = g.statements[n].clone();
    if sql.contains("no_such_table") {
        return (StatusCode::OK, Json(json!({"statement_id": id, "status": {"state": "FAILED",
            "error": {"error_code": "BAD_REQUEST", "message": "[TABLE_OR_VIEW_NOT_FOUND] no_such_table"}}})));
    }
    let result = if sql.ends_with("LIMIT 0") {
        json!({"chunk_index": 0, "row_count": 0})
    } else {
        json!({"external_links": [{
            "chunk_index": 0, "row_offset": 0, "row_count": 2,
            "external_link": format!("/storage/{n}/0"),
            "http_headers": {"x-ms-blob-type": "BlockBlob"},
            "expiration": "2099-01-01T00:00:00Z",
            "next_chunk_index": 1,
            "next_chunk_internal_link": format!("/api/2.0/sql/statements/{id}/result/chunks/1")
        }]})
    };
    (StatusCode::OK, Json(json!({"statement_id": id, "status": {"state": "SUCCEEDED"}, "manifest": manifest(), "result": result})))
}

async fn chunk(State(m): State<M>, h: HeaderMap, Path((id, idx)): Path<(String, u32)>) -> impl IntoResponse {
    assert!(check_auth(&m, &h));
    assert_eq!(idx, 1);
    let n = id.trim_start_matches("st-");
    Json(json!({"chunk_index": 1, "row_offset": 2, "row_count": 2,
        "external_links": [{"chunk_index": 1, "external_link": format!("/storage/{n}/1"),
                            "http_headers": {"x-ms-blob-type": "BlockBlob"}}]}))
}

async fn storage(State(m): State<M>, h: HeaderMap, Path((n, part)): Path<(usize, usize)>) -> impl IntoResponse {
    let mut g = m.lock().unwrap();
    if h.contains_key("authorization") {
        g.leaked_token = true;
    }
    if h.get("x-ms-blob-type").is_none() {
        g.missing_storage_header = true;
    }
    let batch = project(&g.statements[n]);
    // two chunks of two rows each
    ipc(&batch.slice(part * 2, 2))
}

/// The mock returns relative external links; make them absolute for the client.
async fn start_mock() -> (String, M) {
    let m: M = Arc::new(Mutex::new(Mock::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let b2 = base.clone();
    let app = Router::new()
        .route(
            "/api/2.0/sql/statements",
            post(move |s: State<M>, h: HeaderMap, j: Json<Value>| async move { submit(s, h, j).await }),
        )
        .route(
            "/api/2.0/sql/statements/{id}",
            get(move |s: State<M>, h: HeaderMap, p: Path<String>| {
                let b = b2.clone();
                async move {
                    let resp = poll(s, h, p).await.into_response();
                    absolutize(resp, &b).await
                }
            }),
        )
        .route("/api/2.0/sql/statements/{id}/result/chunks/{idx}", {
            let b = base.clone();
            get(move |s: State<M>, h: HeaderMap, p: Path<(String, u32)>| {
                let b = b.clone();
                async move { absolutize(chunk(s, h, p).await.into_response(), &b).await }
            })
        })
        .route("/storage/{n}/{part}", get(storage))
        .with_state(m.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (base, m)
}

async fn absolutize(resp: axum::response::Response, base: &str) -> axum::response::Response {
    let (parts, body) = resp.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap().replace("\"/storage/", &format!("\"{base}/storage/"));
    (parts.status, [("content-type", "application/json")], text).into_response()
}

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn start_gateway(dir: &std::path::Path, mock: &str, table_line: &str) -> (Server, String) {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let cfg = dir.join(format!("cfg-{port}.toml"));
    std::fs::write(
        &cfg,
        format!(
            "[server]\nlisten = \"127.0.0.1:{port}\"\ndata_dir = \"{}\"\n\n[[sources]]\nkind = \"databricks\"\nname = \"sales\"\nhost = \"{mock}\"\nwarehouse_id = \"wh-123\"\n{table_line}\ntoken_env = \"DBX_TOKEN\"\n",
            dir.join("data").display()
        ),
    )
    .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_agentgate"))
        .arg("--config")
        .arg(&cfg)
        .env("DBX_TOKEN", "dapi-test-token")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let base = format!("http://127.0.0.1:{port}");
    let mut child = Some(child);
    for _ in 0..300 {
        if reqwest::get(format!("{base}/health")).await.is_ok() {
            return (Server(child.take().unwrap()), base);
        }
        if let Some(st) = child.as_mut().unwrap().try_wait().unwrap() {
            let mut err = String::new();
            use std::io::Read;
            child.as_mut().unwrap().stderr.take().unwrap().read_to_string(&mut err).ok();
            return (Server(child.take().unwrap()), format!("EXITED {st}: {err}"));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("gateway did not start");
}

async fn sql(base: &str, q: &str) -> Value {
    reqwest::Client::new()
        .post(format!("{base}/v1/query"))
        .json(&json!({ "sql": q }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn databricks_source_end_to_end() {
    let (mock, m) = start_mock().await;
    let dir = tempfile::tempdir().unwrap();
    let (_srv, base) = start_gateway(dir.path(), &mock, "table = \"main.finance.sales\"").await;
    assert!(!base.starts_with("EXITED"), "{base}");

    // schema discovery
    {
        let g = m.lock().unwrap();
        assert_eq!(g.statements[0], "SELECT * FROM `main`.`finance`.`sales` AS t LIMIT 0");
        assert!(g.api_auth_ok);
    }

    // projection + filter pushdown, multi-chunk streaming, decimal + timestamp handling
    let r = sql(&base, "SELECT region, sum(amount) AS total FROM sales WHERE region = 'EU' GROUP BY region").await;
    assert_eq!(r["rows"], json!([{"region": "EU", "total": 125.75}]), "{r}");
    let last = m.lock().unwrap().statements.last().unwrap().clone();
    assert!(last.starts_with("SELECT `region`, `amount` FROM `main`.`finance`.`sales` AS t WHERE"), "{last}");
    assert!(last.contains("`region` = 'EU'"), "{last}");

    // count(*) (empty projection) and limit pushdown
    let r = sql(&base, "SELECT count(*) AS n FROM sales").await;
    assert_eq!(r["rows"][0]["n"], 4, "{r}");
    let r = sql(&base, "SELECT id, ts FROM sales LIMIT 3").await;
    assert_eq!(r["rows"].as_array().unwrap().len(), 3, "{r}");
    assert!(m.lock().unwrap().statements.last().unwrap().ends_with("LIMIT 3"));

    // join a Databricks table with live gateway state
    reqwest::Client::new()
        .put(format!("{base}/v1/agents/analyst/kv/focus_region"))
        .json(&json!("EU"))
        .send()
        .await
        .unwrap();
    let r = sql(
        &base,
        "SELECT k.agent_id, count(*) AS deals FROM sales s JOIN system.agent_kv k \
         ON k.value = concat('\"', s.region, '\"') GROUP BY k.agent_id",
    )
    .await;
    assert_eq!(r["rows"], json!([{"agent_id": "analyst", "deals": 2}]), "{r}");

    let g = m.lock().unwrap();
    assert!(g.polls >= 4, "client must poll PENDING statements");
    assert!(!g.leaked_token, "Databricks token was sent to presigned storage URL");
    assert!(!g.missing_storage_header, "http_headers from external_links were not forwarded");
}

#[tokio::test(flavor = "multi_thread")]
async fn databricks_errors_are_reported() {
    let (mock, _m) = start_mock().await;
    let dir = tempfile::tempdir().unwrap();
    let (_srv, base) = start_gateway(dir.path(), &mock, "table = \"main.no_such_table\"").await;
    assert!(base.starts_with("EXITED"), "startup should fail for a missing table");
    assert!(base.contains("TABLE_OR_VIEW_NOT_FOUND"), "{base}");
}
