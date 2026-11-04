//! Session transcripts: written once to the local Turso outbox (durable, fast),
//! then exported asynchronously to every configured sink:
//!
//! * `duckdb`   – local analytical copy (`--features duckdb`)
//! * `postgres` – cloud system of record
//!
//! Each sink keeps its own cursor, so a sink that is down (e.g. Postgres
//! unreachable from a laptop) catches up automatically when it comes back,
//! without blocking agents or other sinks.

use crate::store::{HubStore, TranscriptEntry};
use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TranscriptConfig {
    #[serde(default)]
    pub sinks: Vec<SinkConfig>,
    /// Export poll interval in milliseconds.
    #[serde(default = "default_interval")]
    pub interval_ms: u64,
}

fn default_interval() -> u64 {
    1000
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(dead_code)]
pub enum SinkConfig {
    Duckdb { path: String },
    Postgres {
        connection: String,
        #[serde(default = "default_pg_table")]
        table: String,
    },
}

fn default_pg_table() -> String {
    "session_transcripts".into()
}

#[async_trait::async_trait]
trait Sink: Send + Sync {
    fn name(&self) -> String;
    async fn write(&self, batch: &[TranscriptEntry]) -> Result<()>;
}

pub fn start(cfg: &TranscriptConfig, store: Arc<HubStore>) -> Result<Vec<String>> {
    let mut names = vec![];
    for s in &cfg.sinks {
        let sink: Arc<dyn Sink> = match s {
            #[cfg(feature = "duckdb")]
            SinkConfig::Duckdb { path } => Arc::new(duck::DuckSink::open(path)?),
            #[cfg(not(feature = "duckdb"))]
            SinkConfig::Duckdb { .. } => return Err(anyhow!("duckdb sink needs `--features duckdb`")),
            #[cfg(feature = "postgres")]
            SinkConfig::Postgres { connection, table } => Arc::new(PgSink::new(connection, table)?),
            #[cfg(not(feature = "postgres"))]
            SinkConfig::Postgres { .. } => return Err(anyhow!("postgres sink needs `--features postgres`")),
        };
        names.push(sink.name());
        tokio::spawn(export_loop(sink, store.clone(), Duration::from_millis(cfg.interval_ms.max(50))));
    }
    Ok(names)
}

async fn export_loop(sink: Arc<dyn Sink>, store: Arc<HubStore>, every: Duration) {
    let name = sink.name();
    let mut backoff = every;
    loop {
        let r: Result<usize> = async {
            let cursor = store.sink_cursor(&name).await?;
            let batch = store.transcripts_after(cursor, 500, None).await?;
            if batch.is_empty() {
                return Ok(0);
            }
            sink.write(&batch).await?;
            store.set_sink_cursor(&name, batch.last().unwrap().seq).await?;
            Ok(batch.len())
        }
        .await;
        match r {
            Ok(n) => {
                backoff = every;
                if n > 0 {
                    tracing::debug!(sink = %name, rows = n, "exported transcripts");
                    continue; // drain quickly
                }
            }
            Err(e) => {
                tracing::warn!(sink = %name, error = %e, "transcript export failed; will retry");
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
        tokio::time::sleep(backoff).await;
    }
}

#[cfg(feature = "postgres")]
struct PgSink {
    pg: crate::pg::Pg,
    table: String,
    ready: tokio::sync::OnceCell<()>,
}

#[cfg(feature = "postgres")]
impl PgSink {
    fn new(conn: &str, table: &str) -> Result<Self> {
        if !table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.') {
            return Err(anyhow!("invalid transcript table name '{table}'"));
        }
        Ok(Self { pg: crate::pg::Pg::new(conn), table: table.to_string(), ready: tokio::sync::OnceCell::new() })
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl Sink for PgSink {
    fn name(&self) -> String {
        format!("postgres:{}", self.table)
    }
    async fn write(&self, batch: &[TranscriptEntry]) -> Result<()> {
        let c = self.pg.client().await?;
        self.ready
            .get_or_try_init(|| async {
                c.batch_execute(&format!(
                    "CREATE TABLE IF NOT EXISTS {t} (
                        gateway_seq BIGINT PRIMARY KEY, session_id TEXT NOT NULL, role TEXT NOT NULL,
                        actor TEXT, content TEXT NOT NULL, metadata JSONB, created_at BIGINT NOT NULL);
                     CREATE INDEX IF NOT EXISTS {ti}_session ON {t} (session_id, gateway_seq);",
                    t = self.table,
                    ti = self.table.replace('.', "_")
                ))
                .await
                .map_err(anyhow::Error::from)
            })
            .await?;
        // ON CONFLICT makes re-delivery after a crash idempotent, so a batch
        // that partially lands is simply retried in full.
        let sql = format!(
            "INSERT INTO {} (gateway_seq, session_id, role, actor, content, metadata, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (gateway_seq) DO NOTHING",
            self.table
        );
        c.batch_execute("BEGIN").await?;
        for e in batch {
            if let Err(err) = c
                .execute(
                    &sql,
                    &[&e.seq, &e.session_id, &e.role, &e.actor, &e.content, &e.metadata, &e.created_at],
                )
                .await
            {
                let _ = c.batch_execute("ROLLBACK").await;
                return Err(err.into());
            }
        }
        c.batch_execute("COMMIT").await?;
        Ok(())
    }
}

#[cfg(feature = "duckdb")]
mod duck {
    use super::*;
    use std::sync::Mutex;

    pub struct DuckSink {
        path: String,
        conn: Arc<Mutex<duckdb::Connection>>,
    }

    impl DuckSink {
        pub fn open(path: &str) -> Result<Self> {
            if let Some(parent) = std::path::Path::new(path).parent() {
                std::fs::create_dir_all(parent).ok();
            }
            let conn = duckdb::Connection::open(path)?;
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS session_transcripts (
                    gateway_seq BIGINT PRIMARY KEY, session_id VARCHAR NOT NULL, role VARCHAR NOT NULL,
                    actor VARCHAR, content VARCHAR NOT NULL, metadata JSON, created_at BIGINT NOT NULL)",
            )?;
            Ok(Self { path: path.to_string(), conn: Arc::new(Mutex::new(conn)) })
        }
    }

    #[async_trait::async_trait]
    impl Sink for DuckSink {
        fn name(&self) -> String {
            format!("duckdb:{}", self.path)
        }
        async fn write(&self, batch: &[TranscriptEntry]) -> Result<()> {
            let conn = self.conn.clone();
            let rows = batch.to_vec();
            tokio::task::spawn_blocking(move || -> Result<()> {
                let mut conn = conn.lock().unwrap();
                let tx = conn.transaction()?;
                {
                    let mut stmt = tx.prepare(
                        "INSERT INTO session_transcripts VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    )?;
                    for e in &rows {
                        stmt.execute(duckdb::params![
                            e.seq,
                            e.session_id,
                            e.role,
                            e.actor,
                            e.content,
                            e.metadata.as_ref().map(|m| m.to_string()),
                            e.created_at
                        ])?;
                    }
                }
                tx.commit()?;
                Ok(())
            })
            .await??;
            Ok(())
        }
    }
}
