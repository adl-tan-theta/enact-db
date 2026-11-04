//! Iceberg end-to-end: a real Iceberg table (metadata, manifests, Parquet data
//! files written by iceberg-rust) served through a mock Iceberg REST catalog
//! that follows the REST spec, queried through enact-db over HTTP and Quack.
#![cfg(feature = "iceberg")]

use arrow_array58::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

type Shared = Arc<Mutex<(Value, String, bool)>>; // (metadata json, metadata location, saw bad auth)

async fn append(catalog: &dyn Catalog, ident: &TableIdent, ids: Vec<i64>, regions: Vec<&str>, amounts: Vec<f64>) {
    let table = catalog.load_table(ident).await.unwrap();
    let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).unwrap());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(regions)),
            Arc::new(Float64Array::from(amounts)),
        ],
    )
    .unwrap();
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        ParquetWriterBuilder::new(
            parquet58::file::properties::WriterProperties::default(),
            table.metadata().current_schema().clone(),
        ),
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata()).unwrap(),
        DefaultFileNameGenerator::new(format!("part-{}", rand_suffix()), None, DataFileFormat::Parquet),
    );
    let mut w = DataFileWriterBuilder::new(rolling).build(None).await.unwrap();
    w.write(batch).await.unwrap();
    let files = w.close().await.unwrap();
    let tx = Transaction::new(&table);
    let tx = tx.fast_append().add_data_files(files).apply(tx).unwrap();
    tx.commit(catalog).await.unwrap();
}

fn rand_suffix() -> u128 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
}

async fn publish(catalog: &dyn Catalog, ident: &TableIdent, shared: &Shared) {
    let t = catalog.load_table(ident).await.unwrap();
    let mut g = shared.lock().unwrap();
    g.0 = serde_json::to_value(t.metadata()).unwrap();
    g.1 = t.metadata_location().unwrap().to_string();
}

fn authed(h: &HeaderMap, s: &Shared) -> bool {
    let ok = h.get("authorization").and_then(|v| v.to_str().ok()) == Some("Bearer lake-token");
    if !ok {
        s.lock().unwrap().2 = true;
    }
    ok
}

async fn start_mock(shared: Shared) -> String {
    let app = Router::new()
        .route("/v1/config", get(|| async { Json(json!({"defaults": {}, "overrides": {}})) }))
        .route(
            "/v1/namespaces",
            get(|State(s): State<Shared>, h: HeaderMap, q: axum::extract::Query<HashMap<String, String>>| async move {
                if !authed(&h, &s) {
                    return (StatusCode::UNAUTHORIZED, Json(json!({"error": {"message": "no", "type": "NotAuthorized", "code": 401}}))).into_response();
                }
                // spec: `parent` lists children; this catalog has one top-level namespace
                if q.contains_key("parent") {
                    return Json(json!({"namespaces": []})).into_response();
                }
                Json(json!({"namespaces": [["sales"]]})).into_response()
            }),
        )
        .route(
            "/v1/namespaces/{ns}/namespaces",
            get(|| async { Json(json!({"namespaces": []})) }),
        )
        .route(
            "/v1/namespaces/{ns}/tables",
            get(|State(s): State<Shared>, h: HeaderMap| async move {
                if !authed(&h, &s) {
                    return StatusCode::UNAUTHORIZED.into_response();
                }
                Json(json!({"identifiers": [{"namespace": ["sales"], "name": "orders"}]})).into_response()
            }),
        )
        .route(
            "/v1/namespaces/{ns}/tables/{t}",
            get(|State(s): State<Shared>, h: HeaderMap| async move {
                if !authed(&h, &s) {
                    return StatusCode::UNAUTHORIZED.into_response();
                }
                let g = s.lock().unwrap();
                Json(json!({"metadata-location": g.1, "metadata": g.0, "config": {}})).into_response()
            }),
        )
        .with_state(shared);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn sql(base: &str, q: &str) -> Value {
    let r = reqwest::Client::new().post(format!("{base}/v1/query")).json(&json!({ "sql": q })).send().await.unwrap();
    r.json().await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_rest_catalog_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    std::fs::create_dir_all(&warehouse).unwrap();

    // 1. a real Iceberg table, written by iceberg-rust
    let catalog = iceberg::memory::MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(iceberg::io::LocalFsStorageFactory))
        .load(
            "mem",
            HashMap::from([(iceberg::memory::MEMORY_CATALOG_WAREHOUSE.to_string(), format!("file://{}", warehouse.display()))]),
        )
        .await
        .unwrap();
    let ns = NamespaceIdent::new("sales".into());
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "region", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "amount", Type::Primitive(PrimitiveType::Double)).into(),
        ])
        .build()
        .unwrap();
    catalog
        .create_table(&ns, TableCreation::builder().name("orders".into()).schema(schema).build())
        .await
        .unwrap();
    let ident = TableIdent::new(ns.clone(), "orders".into());
    append(&catalog, &ident, vec![1, 2, 3], vec!["EU", "US", "EU"], vec![10.0, 20.0, 30.0]).await;
    append(&catalog, &ident, vec![4, 5], vec!["APAC", "EU"], vec![40.0, 50.0]).await;

    // 2. served through a spec-shaped REST catalog
    let shared: Shared = Arc::new(Mutex::new((Value::Null, String::new(), false)));
    publish(&catalog, &ident, &shared).await;
    let mock = start_mock(shared.clone()).await;

    // 3. enact-db mounts it as catalog `lake`
    std::fs::write(dir.path().join("owners.csv"), "region,owner\nEU,analyst\nAPAC,planner\n").unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    std::fs::write(
        dir.path().join("cfg.toml"),
        format!(
            "[server]\nlisten = \"127.0.0.1:{port}\"\ndata_dir = \"{}\"\n\n[[iceberg]]\nname = \"lake\"\nuri = \"{mock}\"\n\
             token_env = \"LAKE_TOKEN\"\nrefresh_secs = 0\n\n[[sources]]\nkind = \"csv\"\nname = \"owners\"\nlocation = \"{}\"\n",
            dir.path().join("data").display(),
            dir.path().join("owners.csv").display()
        ),
    )
    .unwrap();
    let _srv = Server(
        Command::new(env!("CARGO_BIN_EXE_enact-db"))
            .arg("--config")
            .arg(dir.path().join("cfg.toml"))
            .env("LAKE_TOKEN", "lake-token")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let base = format!("http://127.0.0.1:{port}");
    let mut up = false;
    for _ in 0..300 {
        if reqwest::get(format!("{base}/health")).await.is_ok() {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(up, "enact-db did not start");

    // listing, aggregation with pushdown across two data files
    let r = sql(&base, "SELECT table_catalog, table_schema, table_name FROM information_schema.tables WHERE table_catalog = 'lake' AND table_schema = 'sales'").await;
    assert_eq!(r["rows"], json!([{"table_catalog": "lake", "table_schema": "sales", "table_name": "orders"}]), "{r}");
    let r = sql(&base, "SELECT region, sum(amount) AS total, count(*) AS n FROM lake.sales.orders WHERE id >= 2 GROUP BY region ORDER BY region").await;
    assert_eq!(
        r["rows"],
        json!([
            {"region": "APAC", "total": 40.0, "n": 1},
            {"region": "EU", "total": 80.0, "n": 2},
            {"region": "US", "total": 20.0, "n": 1}
        ]),
        "{r}"
    );
    let r = sql(&base, "SELECT count(*) AS n FROM lake.sales.orders").await;
    assert_eq!(r["rows"][0]["n"], 5, "{r}");
    let r = sql(&base, "SELECT id FROM lake.sales.orders WHERE region IN ('APAC') OR amount > 45 ORDER BY id").await;
    assert_eq!(r["rows"], json!([{"id": 4}, {"id": 5}]), "{r}");

    // federated join: Iceberg x CSV
    let r = sql(&base, "SELECT o.owner, sum(t.amount) AS total FROM lake.sales.orders t JOIN owners o ON o.region = t.region GROUP BY o.owner ORDER BY o.owner").await;
    assert_eq!(r["rows"], json!([{"owner": "analyst", "total": 90.0}, {"owner": "planner", "total": 40.0}]), "{r}");

    // a new snapshot is visible on the next query (metadata reloaded per scan)
    append(&catalog, &ident, vec![6], vec!["US"], vec![60.0]).await;
    publish(&catalog, &ident, &shared).await;
    let r = sql(&base, "SELECT count(*) AS n, max(id) AS m FROM lake.sales.orders").await;
    assert_eq!(r["rows"][0], json!({"n": 6, "m": 6}), "{r}");

    // Quack: the Iceberg namespace is exposed as nested schema lake.sales
    let url = format!("{base}/quack");
    let http = reqwest::Client::new();
    let post = |body: Vec<u8>| {
        let http = http.clone();
        let url = url.clone();
        async move { http.post(url).body(body).send().await.unwrap().bytes().await.unwrap().to_vec() }
    };
    // CONNECT (no token configured)
    let mut c = vec![1, 0, 1, 3, 0];
    c.extend([0xFF; 9]);
    c.extend([0x01, 0xFF, 0xFF, 4, 0, 3, 5, 0, 3, 7, 0, 60, 0xFF, 0xFF]);
    let resp = post(c).await;
    assert_eq!(&resp[..3], &[1, 0, 2], "connection response");
    let id_len = resp[5] as usize;
    let conn = String::from_utf8(resp[6..6 + id_len].to_vec()).unwrap();
    let prepare = |sql: &str| {
        let mut m = vec![1, 0, 3, 2, 0, conn.len() as u8];
        m.extend(conn.as_bytes());
        m.extend([3, 0]);
        m.extend([0xFF; 9]);
        m.extend([0x01, 0xFF, 0xFF]);
        m.extend([1, 0]);
        m.push(sql.len() as u8); // < 128 bytes
        m.extend(sql.as_bytes());
        m.extend([2, 0, 0, 1, 0xFF, 0xFF]); // uuid (0, 1)
        m
    };
    let r = post(prepare("SELECT schema_oid, sql, 'table' FROM duckdb_tables()")).await;
    assert_eq!(&r[..3], &[1, 0, 4], "prepare response");
    let text = String::from_utf8_lossy(&r);
    assert!(text.contains("CREATE TABLE \"orders\"(\"id\" BIGINT, \"region\" VARCHAR, \"amount\" DOUBLE);"), "{text}");
    let r = post(prepare("SELECT #2 FROM lake.sales.orders")).await;
    assert_eq!(&r[..3], &[1, 0, 4], "scan of lake.sales.orders over quack: {}", String::from_utf8_lossy(&r));
    assert!(String::from_utf8_lossy(&r).contains("APAC"));

    assert!(!shared.lock().unwrap().2, "catalog requests must carry the configured bearer token");
}
