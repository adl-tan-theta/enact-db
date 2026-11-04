//! The gateway's own local database (`<data_dir>/hub.db`, Turso/libSQL format).
//!
//! Tables:
//! * `crdt_updates`  – append-only Yjs update log per document (compacted to snapshots)
//! * `memories`      – local agent memory with Turso-native vectors (`F32_BLOB`)
//! * `transcripts`   – session transcript outbox (fan-out to DuckDB/Postgres sinks)
//! * `sink_cursors`  – per-sink export position for the transcript outbox
//!
//! NOTE (turso 0.4.4): with an index on `(x, rowid_alias)`, a predicate
//! `x = ? AND rowid_alias <op> ?` returns no rows. We write `+seq` to keep the
//! planner off that index path until upstream fixes it.
//!
//! All access goes through one connection guarded by an async mutex: the gateway
//! is the only process that opens this file, so there is no lock contention.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use turso::{Connection, Value};
use yrs::{ReadTxn, StateVector, Transact};

use crate::agents::now_secs;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS crdt_updates (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id     TEXT NOT NULL,
    data       BLOB NOT NULL,
    origin     TEXT,
    is_snapshot INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS crdt_updates_doc ON crdt_updates(doc_id, seq);
CREATE TABLE IF NOT EXISTS memories (
    namespace  TEXT NOT NULL,
    id         TEXT NOT NULL,
    content    TEXT NOT NULL,
    metadata   TEXT,
    embedding  BLOB,
    agent_id   TEXT,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (namespace, id)
);
CREATE TABLE IF NOT EXISTS transcripts (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    role       TEXT NOT NULL,
    actor      TEXT,
    content    TEXT NOT NULL,
    metadata   TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS transcripts_session ON transcripts(session_id, seq);
CREATE TABLE IF NOT EXISTS sink_cursors (
    sink     TEXT PRIMARY KEY,
    last_seq INTEGER NOT NULL
);
"#;

struct Append {
    doc_id: String,
    data: Vec<u8>,
    origin: String,
    doc: yrs::Doc,
}

pub struct HubStore {
    conn: Mutex<Connection>,
    appends: mpsc::UnboundedSender<Append>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptEntry {
    #[serde(default)]
    pub seq: i64,
    pub session_id: String,
    pub role: String,
    #[serde(default)]
    pub actor: Option<String>,
    pub content: String,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DocStat {
    pub doc_id: String,
    pub log_entries: i64,
    pub bytes: i64,
    pub last_update: i64,
}

impl HubStore {
    pub async fn open(data_dir: &Path, compact_after: u64) -> Result<Arc<Self>> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join("hub.db");
        let db = turso::Builder::new_local(&path.to_string_lossy()).build().await?;
        let conn = db.connect()?;
        conn.execute_batch(SCHEMA).await?;
        let (tx, rx) = mpsc::unbounded_channel();
        let store = Arc::new(Self { conn: Mutex::new(conn), appends: tx });
        tokio::spawn(Self::writer(store.clone(), rx, compact_after.max(1)));
        tracing::info!(path = %path.display(), "hub store ready");
        Ok(store)
    }

    // ---- CRDT log ------------------------------------------------------------

    /// Called from the (synchronous) yrs observer: queue an update for the writer task.
    pub fn append(&self, doc_id: &str, data: Vec<u8>, origin: String, doc: yrs::Doc) {
        let _ = self.appends.send(Append { doc_id: doc_id.to_string(), data, origin, doc });
    }

    async fn writer(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<Append>, compact_after: u64) {
        let mut since_compact: HashMap<String, u64> = HashMap::new();
        while let Some(a) = rx.recv().await {
            let res = {
                let conn = self.conn.lock().await;
                conn.execute(
                    "INSERT INTO crdt_updates (doc_id, data, origin, created_at) VALUES (?1, ?2, ?3, ?4)",
                    (a.doc_id.as_str(), a.data, a.origin.as_str(), now_secs()),
                )
                .await
            };
            if let Err(e) = res {
                tracing::error!(doc = %a.doc_id, error = %e, "failed to persist CRDT update");
                continue;
            }
            let n = since_compact.entry(a.doc_id.clone()).or_default();
            *n += 1;
            if *n >= compact_after {
                *n = 0;
                if let Err(e) = self.compact(&a.doc_id, a.doc).await {
                    tracing::error!(doc = %a.doc_id, error = %e, "compaction failed");
                }
            }
        }
    }

    /// Replace a document's log with a single snapshot. Updates written after
    /// the snapshot's max seq are kept; re-applying overlapping updates is a
    /// no-op for CRDTs, so this is safe without stopping writers.
    async fn compact(&self, doc_id: &str, doc: yrs::Doc) -> Result<()> {
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query("SELECT COALESCE(MAX(seq), 0) FROM crdt_updates WHERE doc_id = ?1", (doc_id,))
            .await?;
        let max_seq = match rows.next().await? {
            Some(r) => r.get::<i64>(0)?,
            None => 0,
        };
        drop(rows);
        let snapshot = tokio::task::spawn_blocking(move || {
            doc.transact().encode_state_as_update_v1(&StateVector::default())
        })
        .await?;
        conn.execute("BEGIN", ()).await?;
        let r: Result<()> = async {
            conn.execute(
                "INSERT INTO crdt_updates (doc_id, data, origin, is_snapshot, created_at) VALUES (?1, ?2, 'snapshot', 1, ?3)",
                (doc_id, snapshot, now_secs()),
            )
            .await?;
            conn.execute("DELETE FROM crdt_updates WHERE doc_id = ?1 AND +seq <= ?2", (doc_id, max_seq))
                .await?;
            Ok(())
        }
        .await;
        match r {
            Ok(()) => {
                conn.execute("COMMIT", ()).await?;
                tracing::info!(doc = doc_id, upto = max_seq, "compacted CRDT log");
                Ok(())
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                Err(e)
            }
        }
    }

    pub async fn load_doc(&self, doc_id: &str) -> Result<Vec<Vec<u8>>> {
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query("SELECT data FROM crdt_updates WHERE doc_id = ?1 ORDER BY seq", (doc_id,))
            .await?;
        let mut out = vec![];
        while let Some(r) = rows.next().await? {
            match r.get_value(0)? {
                Value::Blob(b) => out.push(b),
                other => return Err(anyhow!("unexpected crdt_updates.data value {other:?}")),
            }
        }
        Ok(out)
    }

    pub async fn doc_exists(&self, doc_id: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let mut rows = conn.query("SELECT 1 FROM crdt_updates WHERE doc_id = ?1 LIMIT 1", (doc_id,)).await?;
        Ok(rows.next().await?.is_some())
    }

    pub async fn session_exists(&self, session: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let mut rows = conn.query("SELECT 1 FROM transcripts WHERE session_id = ?1 LIMIT 1", (session,)).await?;
        Ok(rows.next().await?.is_some())
    }

    pub async fn doc_stats(&self) -> Result<Vec<DocStat>> {
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "SELECT doc_id, COUNT(*), SUM(LENGTH(data)), MAX(created_at) FROM crdt_updates GROUP BY doc_id ORDER BY doc_id",
                (),
            )
            .await?;
        let mut out = vec![];
        while let Some(r) = rows.next().await? {
            out.push(DocStat {
                doc_id: r.get(0)?,
                log_entries: r.get(1)?,
                bytes: r.get(2)?,
                last_update: r.get(3)?,
            });
        }
        Ok(out)
    }

    // ---- local memory (Turso vectors) -------------------------------------------

    pub async fn memory_upsert(&self, m: &crate::memory::MemoryRecord) -> Result<()> {
        let conn = self.conn.lock().await;
        let embedding = match &m.embedding {
            Some(v) => Value::Text(vec_literal(v)),
            None => Value::Null,
        };
        conn.execute(
            "INSERT INTO memories (namespace, id, content, metadata, embedding, agent_id, created_at)
             VALUES (?1, ?2, ?3, ?4, CASE WHEN ?5 IS NULL THEN NULL ELSE vector32(?5) END, ?6, ?7)
             ON CONFLICT(namespace, id) DO UPDATE SET content = excluded.content, metadata = excluded.metadata,
                embedding = excluded.embedding, agent_id = excluded.agent_id, created_at = excluded.created_at",
            (
                m.namespace.as_str(),
                m.id.as_str(),
                m.content.as_str(),
                m.metadata.as_ref().map(|v| v.to_string()),
                embedding,
                m.agent_id.clone(),
                if m.created_at == 0 { now_secs() } else { m.created_at },
            ),
        )
        .await?;
        Ok(())
    }

    pub async fn memory_search(&self, q: &crate::memory::MemoryQuery) -> Result<Vec<crate::memory::MemoryHit>> {
        let conn = self.conn.lock().await;
        let k = q.k.clamp(1, 1000) as i64;
        let mut rows = match (&q.embedding, &q.text) {
            (Some(v), _) => {
                conn.query(
                    "SELECT namespace, id, content, metadata, agent_id, created_at,
                            vector_distance_cos(embedding, vector32(?2)) AS dist
                     FROM memories WHERE namespace = ?1 AND embedding IS NOT NULL
                     ORDER BY dist ASC LIMIT ?3",
                    (q.namespace.as_str(), vec_literal(v), k),
                )
                .await?
            }
            (None, Some(t)) => {
                conn.query(
                    "SELECT namespace, id, content, metadata, agent_id, created_at, NULL
                     FROM memories WHERE namespace = ?1 AND content LIKE ?2
                     ORDER BY created_at DESC LIMIT ?3",
                    (q.namespace.as_str(), format!("%{t}%"), k),
                )
                .await?
            }
            (None, None) => {
                conn.query(
                    "SELECT namespace, id, content, metadata, agent_id, created_at, NULL
                     FROM memories WHERE namespace = ?1 ORDER BY created_at DESC LIMIT ?2",
                    (q.namespace.as_str(), k),
                )
                .await?
            }
        };
        let mut out = vec![];
        while let Some(r) = rows.next().await? {
            let metadata: Option<String> = r.get(3)?;
            let dist = match r.get_value(6)? {
                Value::Real(f) => Some(f),
                Value::Integer(i) => Some(i as f64),
                _ => None,
            };
            out.push(crate::memory::MemoryHit {
                namespace: r.get(0)?,
                id: r.get(1)?,
                content: r.get(2)?,
                metadata: metadata.and_then(|s| serde_json::from_str(&s).ok()),
                agent_id: r.get(4)?,
                created_at: r.get(5)?,
                distance: dist,
            });
        }
        Ok(out)
    }

    pub async fn memory_delete(&self, namespace: &str, id: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn
            .execute("DELETE FROM memories WHERE namespace = ?1 AND id = ?2", (namespace, id))
            .await?;
        Ok(n > 0)
    }

    pub async fn memory_all(&self) -> Result<Vec<crate::memory::MemoryHit>> {
        self.memory_scan("SELECT namespace, id, content, metadata, agent_id, created_at, NULL FROM memories ORDER BY namespace, created_at")
            .await
    }

    async fn memory_scan(&self, sql: &str) -> Result<Vec<crate::memory::MemoryHit>> {
        let conn = self.conn.lock().await;
        let mut rows = conn.query(sql, ()).await?;
        let mut out = vec![];
        while let Some(r) = rows.next().await? {
            let metadata: Option<String> = r.get(3)?;
            out.push(crate::memory::MemoryHit {
                namespace: r.get(0)?,
                id: r.get(1)?,
                content: r.get(2)?,
                metadata: metadata.and_then(|s| serde_json::from_str(&s).ok()),
                agent_id: r.get(4)?,
                created_at: r.get(5)?,
                distance: None,
            });
        }
        Ok(out)
    }

    // ---- transcripts outbox -------------------------------------------------------

    pub async fn transcript_append(&self, e: &TranscriptEntry) -> Result<i64> {
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "INSERT INTO transcripts (session_id, role, actor, content, metadata, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING seq",
                (
                    e.session_id.as_str(),
                    e.role.as_str(),
                    e.actor.clone(),
                    e.content.as_str(),
                    e.metadata.as_ref().map(|v| v.to_string()),
                    if e.created_at == 0 { now_secs() } else { e.created_at },
                ),
            )
            .await?;
        let seq = rows.next().await?.ok_or_else(|| anyhow!("no seq returned"))?.get::<i64>(0)?;
        Ok(seq)
    }

    pub async fn transcripts_after(&self, after_seq: i64, limit: i64, session: Option<&str>) -> Result<Vec<TranscriptEntry>> {
        let conn = self.conn.lock().await;
        let mut rows = match session {
            Some(s) => {
                conn.query(
                    "SELECT seq, session_id, role, actor, content, metadata, created_at FROM transcripts
                     WHERE +seq > ?1 AND session_id = ?2 ORDER BY seq LIMIT ?3",
                    (after_seq, s, limit),
                )
                .await?
            }
            None => {
                conn.query(
                    "SELECT seq, session_id, role, actor, content, metadata, created_at FROM transcripts
                     WHERE seq > ?1 ORDER BY seq LIMIT ?2",
                    (after_seq, limit),
                )
                .await?
            }
        };
        let mut out = vec![];
        while let Some(r) = rows.next().await? {
            let metadata: Option<String> = r.get(5)?;
            out.push(TranscriptEntry {
                seq: r.get(0)?,
                session_id: r.get(1)?,
                role: r.get(2)?,
                actor: r.get(3)?,
                content: r.get(4)?,
                metadata: metadata.and_then(|s| serde_json::from_str(&s).ok()),
                created_at: r.get(6)?,
            });
        }
        Ok(out)
    }

    pub async fn sink_cursor(&self, sink: &str) -> Result<i64> {
        let conn = self.conn.lock().await;
        let mut rows = conn.query("SELECT last_seq FROM sink_cursors WHERE sink = ?1", (sink,)).await?;
        Ok(match rows.next().await? {
            Some(r) => r.get(0)?,
            None => 0,
        })
    }

    pub async fn set_sink_cursor(&self, sink: &str, seq: i64) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO sink_cursors (sink, last_seq) VALUES (?1, ?2)
             ON CONFLICT(sink) DO UPDATE SET last_seq = excluded.last_seq",
            (sink, seq),
        )
        .await?;
        Ok(())
    }
}

pub fn vec_literal(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|f| format!("{f}")).collect();
    format!("[{}]", parts.join(","))
}
