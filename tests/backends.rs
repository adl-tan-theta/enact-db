//! Integration tests for the cloud / optional backends against real services.
//!
//! Requires a Postgres with pgvector:
//!   ENACT_DB_TEST_PG="host=127.0.0.1 user=agent password=agentpw dbname=agents" \
//!   cargo test --features full --test backends
//! Skips (passes trivially) when ENACT_DB_TEST_PG is unset.
#![cfg(feature = "postgres")]

use serde_json::{json, Value};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct Server(Child, String);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn start(dir: &std::path::Path, toml: &str) -> Server {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let cfg = dir.join(format!("cfg-{port}.toml"));
    std::fs::write(
        &cfg,
        format!("[server]\nlisten = \"127.0.0.1:{port}\"\ndata_dir = \"{}\"\n{toml}", dir.join("data").display()),
    )
    .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_enact-db"))
        .arg("--config")
        .arg(&cfg)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let base = format!("http://127.0.0.1:{port}");
    for _ in 0..400 {
        if reqwest::get(format!("{base}/health")).await.is_ok() {
            return Server(child, base);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server did not start");
}

async fn pg() -> Option<(tokio_postgres::Client, String)> {
    let conn = std::env::var("ENACT_DB_TEST_PG").ok()?;
    let (c, task) = tokio_postgres::connect(&conn, tokio_postgres::NoTls).await.expect("connect test pg");
    tokio::spawn(task);
    Some((c, conn))
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_source_memory_and_transcripts() {
    let Some((pgc, conn)) = pg().await else {
        eprintln!("ENACT_DB_TEST_PG not set; skipping");
        return;
    };
    pgc.batch_execute(
        "DROP TABLE IF EXISTS it_memories; DROP TABLE IF EXISTS it_transcripts;
         CREATE TABLE IF NOT EXISTS crm_customers (id int primary key, name text, tier text, owner_agent text);
         INSERT INTO crm_customers VALUES (1,'Acme','gold','researcher'),(2,'Globex','silver','writer'),
           (3,'Initech','gold','writer') ON CONFLICT DO NOTHING;",
    )
    .await
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let duck_sink = if cfg!(feature = "duckdb") {
        format!("[[transcripts.sinks]]\nkind = \"duckdb\"\npath = \"{}\"\n", dir.path().join("t.duckdb").display())
    } else {
        String::new()
    };
    let toml = format!(
        r#"
[memory]
backend = "postgres"
postgres = "{conn}"
postgres_table = "it_memories"

[transcripts]
interval_ms = 100
[[transcripts.sinks]]
kind = "postgres"
connection = "{conn}"
table = "it_transcripts"
{duck_sink}
[[sources]]
kind = "postgres"
name = "customers"
connection = "{conn}"
table = "public.crm_customers"
"#
    );
    let srv = start(dir.path(), &toml).await;
    let b = &srv.1;
    let http = reqwest::Client::new();

    // agent activity, then a federated join: Postgres table x live AgentFS audit
    for agent in ["writer", "writer", "researcher"] {
        http.put(format!("{b}/v1/agents/{agent}/kv/last"))
            .header("x-agent-id", agent)
            .json(&json!(1))
            .send()
            .await
            .unwrap();
    }
    let q: Value = http
        .post(format!("{b}/v1/query"))
        .json(&json!({"sql": "SELECT c.name, c.tier, count(t.id) AS actions FROM customers c \
            JOIN system.agent_tool_calls t ON t.agent_id = c.owner_agent \
            WHERE c.tier = 'gold' GROUP BY c.name, c.tier ORDER BY c.name"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(q["rows"], json!([{"name":"Acme","tier":"gold","actions":1},{"name":"Initech","tier":"gold","actions":2}]), "{q}");

    // pgvector memory
    let r = http
        .post(format!("{b}/v1/memory"))
        .json(&json!([
            {"namespace": "kb", "id": "a", "content": "alpha", "embedding": [1.0, 0.0]},
            {"namespace": "kb", "id": "b", "content": "beta", "embedding": [0.0, 1.0]}
        ]))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    let hits: Value = http
        .post(format!("{b}/v1/memory/search"))
        .json(&json!({"namespace": "kb", "embedding": [0.2, 0.9], "k": 1}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(hits["hits"][0]["id"], "b", "{hits}");
    let n: i64 = pgc.query_one("SELECT count(*) FROM it_memories", &[]).await.unwrap().get(0);
    assert_eq!(n, 2);

    // transcripts -> Postgres (and DuckDB) via the outbox
    http.post(format!("{b}/v1/transcripts"))
        .json(&json!([
            {"session_id": "s9", "role": "user", "content": "hello", "metadata": {"ui": "web"}},
            {"session_id": "s9", "role": "assistant", "content": "hi there"}
        ]))
        .send()
        .await
        .unwrap();
    let mut got = 0i64;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Ok(row) = pgc.query_one("SELECT count(*) FROM it_transcripts WHERE session_id = 's9'", &[]).await {
            got = row.get(0);
            if got == 2 {
                break;
            }
        }
    }
    assert_eq!(got, 2, "transcripts exported to postgres");
    drop(srv);

    #[cfg(feature = "duckdb")]
    {
        let c = duckdb::Connection::open(dir.path().join("t.duckdb")).unwrap();
        let n: i64 = c
            .query_row("SELECT count(*) FROM session_transcripts WHERE session_id = 's9'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2, "transcripts exported to duckdb");
    }
}

#[cfg(feature = "lancedb")]
#[tokio::test(flavor = "multi_thread")]
async fn lancedb_memory() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        "[memory]\nbackend = \"lancedb\"\nlancedb_uri = \"{}\"\ndimensions = 3\n",
        dir.path().join("lance").display()
    );
    let srv = start(dir.path(), &toml).await;
    let b = &srv.1;
    let http = reqwest::Client::new();
    let r = http
        .post(format!("{b}/v1/memory"))
        .json(&json!([
            {"namespace": "kb", "id": "cats", "content": "cats purr", "embedding": [1.0, 0.0, 0.0]},
            {"namespace": "kb", "id": "dogs", "content": "dogs bark", "embedding": [0.0, 1.0, 0.0]},
            {"namespace": "other", "id": "dogs2", "content": "dogs", "embedding": [0.0, 1.0, 0.0]}
        ]))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    // upsert replaces
    http.post(format!("{b}/v1/memory"))
        .json(&json!({"namespace": "kb", "id": "dogs", "content": "dogs bark loudly", "embedding": [0.0, 1.0, 0.0]}))
        .send()
        .await
        .unwrap();
    let hits: Value = http
        .post(format!("{b}/v1/memory/search"))
        .json(&json!({"namespace": "kb", "embedding": [0.1, 0.9, 0.0], "k": 5}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ids: Vec<&str> = hits["hits"].as_array().unwrap().iter().map(|h| h["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["dogs", "cats"], "{hits}");
    assert_eq!(hits["hits"][0]["content"], "dogs bark loudly");
}
