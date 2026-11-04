//! Agent/human memory: upsert + similarity search over namespaced records.
//!
//! Backends (pick with `[memory] backend = ...`):
//! * `turso`    – local, zero-dependency, vectors via Turso's `vector32` / `vector_distance_cos` (default)
//! * `postgres` – cloud, pgvector (`<=>` cosine distance); needs `CREATE EXTENSION vector`
//! * `lancedb`  – local or object-store LanceDB dataset (build with `--features lancedb`)
//!
//! Embeddings are supplied by the caller (agents already have an embedding
//! model in their loop); text-only queries fall back to substring matching.

use crate::store::HubStore;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryRecord {
    #[serde(default)]
    pub namespace: String,
    pub id: String,
    pub content: String,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub created_at: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryQuery {
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default = "default_k")]
    pub k: usize,
}

fn default_k() -> usize {
    10
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryHit {
    pub namespace: String,
    pub id: String,
    pub content: String,
    pub metadata: Option<serde_json::Value>,
    pub agent_id: Option<String>,
    pub created_at: i64,
    /// Cosine distance (0 = identical); absent for non-vector queries.
    pub distance: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[allow(dead_code)] // some fields only used with optional features
pub struct MemoryConfig {
    #[serde(default = "default_backend")]
    pub backend: String,
    /// Postgres connection string (backend = "postgres").
    pub postgres: Option<String>,
    /// Table name for the Postgres backend.
    #[serde(default)]
    pub postgres_table: Option<String>,
    /// LanceDB URI, e.g. "./data/lancedb" or "az://container/lancedb" (backend = "lancedb").
    pub lancedb_uri: Option<String>,
    /// Embedding dimension; required by LanceDB (fixed-size vectors).
    pub dimensions: Option<usize>,
}

fn default_backend() -> String {
    "turso".into()
}

pub enum MemoryStore {
    Turso(Arc<HubStore>),
    #[cfg(feature = "postgres")]
    Postgres { pg: crate::pg::Pg, table: String },
    #[cfg(feature = "lancedb")]
    Lance(lance::LanceMemory),
}

impl MemoryStore {
    pub async fn open(cfg: &MemoryConfig, hub: Arc<HubStore>) -> Result<Self> {
        match cfg.backend.as_str() {
            "" | "turso" => Ok(MemoryStore::Turso(hub)),
            #[cfg(feature = "postgres")]
            "postgres" => {
                let conn = cfg.postgres.as_deref().ok_or_else(|| anyhow!("[memory] postgres = \"...\" required"))?;
                let table = cfg.postgres_table.clone().unwrap_or_else(|| "agent_memories".into());
                if !table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.') {
                    return Err(anyhow!("invalid postgres_table name"));
                }
                let pg = crate::pg::Pg::new(conn);
                let client = pg.client().await?;
                if let Err(e) = client.batch_execute("CREATE EXTENSION IF NOT EXISTS vector").await {
                    tracing::warn!(error = %e, "could not create pgvector extension (may already exist / need superuser)");
                }
                client
                    .batch_execute(&format!(
                        "CREATE TABLE IF NOT EXISTS {table} (
                            namespace TEXT NOT NULL, id TEXT NOT NULL, content TEXT NOT NULL,
                            metadata JSONB, embedding vector, agent_id TEXT,
                            created_at BIGINT NOT NULL, PRIMARY KEY (namespace, id))"
                    ))
                    .await?;
                Ok(MemoryStore::Postgres { pg, table })
            }
            #[cfg(feature = "lancedb")]
            "lancedb" => {
                let uri = cfg.lancedb_uri.as_deref().ok_or_else(|| anyhow!("[memory] lancedb_uri required"))?;
                let dims = cfg.dimensions.ok_or_else(|| anyhow!("[memory] dimensions required for lancedb"))?;
                Ok(MemoryStore::Lance(lance::LanceMemory::open(uri, dims).await?))
            }
            other => Err(anyhow!(
                "memory backend '{other}' unknown or not compiled in (turso | postgres | lancedb)"
            )),
        }
    }

    pub fn backend_name(&self) -> &'static str {
        match self {
            MemoryStore::Turso(_) => "turso",
            #[cfg(feature = "postgres")]
            MemoryStore::Postgres { .. } => "postgres",
            #[cfg(feature = "lancedb")]
            MemoryStore::Lance(_) => "lancedb",
        }
    }

    pub async fn upsert(&self, m: &MemoryRecord) -> Result<()> {
        if m.namespace.is_empty() || m.id.is_empty() {
            return Err(anyhow!("namespace and id are required"));
        }
        match self {
            MemoryStore::Turso(h) => h.memory_upsert(m).await,
            #[cfg(feature = "postgres")]
            MemoryStore::Postgres { pg, table } => {
                let c = pg.client().await?;
                let emb = m.embedding.as_ref().map(|v| crate::store::vec_literal(v));
                c.execute(
                    &format!(
                        "INSERT INTO {table} (namespace, id, content, metadata, embedding, agent_id, created_at)
                         VALUES ($1, $2, $3, $4, $5::text::vector, $6, $7)
                         ON CONFLICT (namespace, id) DO UPDATE SET content = EXCLUDED.content,
                           metadata = EXCLUDED.metadata, embedding = EXCLUDED.embedding,
                           agent_id = EXCLUDED.agent_id, created_at = EXCLUDED.created_at"
                    ),
                    &[
                        &m.namespace,
                        &m.id,
                        &m.content,
                        &m.metadata,
                        &emb,
                        &m.agent_id,
                        &(if m.created_at == 0 { crate::agents::now_secs() } else { m.created_at }),
                    ],
                )
                .await?;
                Ok(())
            }
            #[cfg(feature = "lancedb")]
            MemoryStore::Lance(l) => l.upsert(m).await,
        }
    }

    pub async fn search(&self, q: &MemoryQuery) -> Result<Vec<MemoryHit>> {
        match self {
            MemoryStore::Turso(h) => h.memory_search(q).await,
            #[cfg(feature = "postgres")]
            MemoryStore::Postgres { pg, table } => {
                let c = pg.client().await?;
                let k = q.k.clamp(1, 1000) as i64;
                let rows = match (&q.embedding, &q.text) {
                    (Some(v), _) => {
                        c.query(
                            &format!(
                                "SELECT namespace, id, content, metadata, agent_id, created_at,
                                        (embedding <=> $2::text::vector)::float8 AS dist
                                 FROM {table} WHERE namespace = $1 AND embedding IS NOT NULL
                                 ORDER BY embedding <=> $2::text::vector LIMIT $3"
                            ),
                            &[&q.namespace, &crate::store::vec_literal(v), &k],
                        )
                        .await?
                    }
                    (None, Some(t)) => {
                        c.query(
                            &format!(
                                "SELECT namespace, id, content, metadata, agent_id, created_at, NULL::float8
                                 FROM {table} WHERE namespace = $1 AND content ILIKE $2
                                 ORDER BY created_at DESC LIMIT $3"
                            ),
                            &[&q.namespace, &format!("%{t}%"), &k],
                        )
                        .await?
                    }
                    (None, None) => {
                        c.query(
                            &format!(
                                "SELECT namespace, id, content, metadata, agent_id, created_at, NULL::float8
                                 FROM {table} WHERE namespace = $1 ORDER BY created_at DESC LIMIT $2"
                            ),
                            &[&q.namespace, &k],
                        )
                        .await?
                    }
                };
                Ok(rows
                    .iter()
                    .map(|r| MemoryHit {
                        namespace: r.get(0),
                        id: r.get(1),
                        content: r.get(2),
                        metadata: r.get(3),
                        agent_id: r.get(4),
                        created_at: r.get(5),
                        distance: r.get(6),
                    })
                    .collect())
            }
            #[cfg(feature = "lancedb")]
            MemoryStore::Lance(l) => l.search(q).await,
        }
    }

    pub async fn delete(&self, namespace: &str, id: &str) -> Result<bool> {
        match self {
            MemoryStore::Turso(h) => h.memory_delete(namespace, id).await,
            #[cfg(feature = "postgres")]
            MemoryStore::Postgres { pg, table } => {
                let c = pg.client().await?;
                let n = c
                    .execute(&format!("DELETE FROM {table} WHERE namespace = $1 AND id = $2"), &[&namespace, &id])
                    .await?;
                Ok(n > 0)
            }
            #[cfg(feature = "lancedb")]
            MemoryStore::Lance(l) => l.delete(namespace, id).await,
        }
    }
}

#[cfg(feature = "lancedb")]
mod lance {
    use super::{MemoryHit, MemoryQuery, MemoryRecord};
    use anyhow::{anyhow, Result};
    use arrow_array58::types::Float32Type;
    use arrow_array58::{
        Array, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
    };
    use arrow_schema58::{DataType, Field, Schema, SchemaRef};
    use futures::TryStreamExt;
    use lancedb::query::{ExecutableQuery, QueryBase};
    use std::sync::Arc;

    const TABLE: &str = "memories";

    pub struct LanceMemory {
        table: lancedb::Table,
        schema: SchemaRef,
        dims: usize,
    }

    fn esc(s: &str) -> String {
        s.replace('\'', "''")
    }

    impl LanceMemory {
        pub async fn open(uri: &str, dims: usize) -> Result<Self> {
            let schema = Arc::new(Schema::new(vec![
                Field::new("namespace", DataType::Utf8, false),
                Field::new("id", DataType::Utf8, false),
                Field::new("content", DataType::Utf8, false),
                Field::new("metadata", DataType::Utf8, true),
                Field::new("agent_id", DataType::Utf8, true),
                Field::new("created_at", DataType::Int64, false),
                Field::new(
                    "vector",
                    DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dims as i32),
                    true,
                ),
            ]));
            let db = lancedb::connect(uri).execute().await?;
            let names = db.table_names().execute().await?;
            let table = if names.iter().any(|n| n == TABLE) {
                db.open_table(TABLE).execute().await?
            } else {
                db.create_empty_table(TABLE, schema.clone()).execute().await?
            };
            tracing::info!(uri, dims, "lancedb memory ready");
            Ok(Self { table, schema, dims })
        }

        pub async fn upsert(&self, m: &MemoryRecord) -> Result<()> {
            if let Some(v) = &m.embedding {
                if v.len() != self.dims {
                    return Err(anyhow!("embedding has {} dims, table expects {}", v.len(), self.dims));
                }
            }
            let vector = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                std::iter::once(m.embedding.as_ref().map(|v| v.iter().map(|f| Some(*f)).collect::<Vec<_>>())),
                self.dims as i32,
            );
            let batch = RecordBatch::try_new(
                self.schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![m.namespace.clone()])),
                    Arc::new(StringArray::from(vec![m.id.clone()])),
                    Arc::new(StringArray::from(vec![m.content.clone()])),
                    Arc::new(StringArray::from(vec![m.metadata.as_ref().map(|v| v.to_string())])),
                    Arc::new(StringArray::from(vec![m.agent_id.clone()])),
                    Arc::new(Int64Array::from(vec![if m.created_at == 0 {
                        crate::agents::now_secs()
                    } else {
                        m.created_at
                    }])),
                    Arc::new(vector),
                ],
            )?;
            self.delete(&m.namespace, &m.id).await?;
            self.table.add(batch).execute().await?;
            Ok(())
        }

        pub async fn delete(&self, namespace: &str, id: &str) -> Result<bool> {
            self.table
                .delete(&format!("namespace = '{}' AND id = '{}'", esc(namespace), esc(id)))
                .await?;
            Ok(true)
        }

        pub async fn search(&self, q: &MemoryQuery) -> Result<Vec<MemoryHit>> {
            let mut filter = format!("namespace = '{}'", esc(&q.namespace));
            if let Some(t) = &q.text {
                filter.push_str(&format!(" AND content LIKE '%{}%'", esc(t)));
            }
            let k = q.k.clamp(1, 1000);
            let batches: Vec<RecordBatch> = match &q.embedding {
                Some(v) => {
                    self.table
                        .query()
                        .only_if(filter)
                        .limit(k)
                        .nearest_to(v.as_slice())?
                        .distance_type(lancedb::DistanceType::Cosine)
                        .execute()
                        .await?
                        .try_collect()
                        .await?
                }
                None => self.table.query().only_if(filter).limit(k).execute().await?.try_collect().await?,
            };
            let mut out = vec![];
            for b in batches {
                let col = |name: &str| b.column_by_name(name).cloned();
                let s = |name: &str| -> Option<StringArray> {
                    col(name).and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned())
                };
                let (ns, id, content, meta, agent) =
                    (s("namespace"), s("id"), s("content"), s("metadata"), s("agent_id"));
                let created = col("created_at").and_then(|c| c.as_any().downcast_ref::<Int64Array>().cloned());
                let dist = col("_distance").and_then(|c| c.as_any().downcast_ref::<Float32Array>().cloned());
                for i in 0..b.num_rows() {
                    let get = |a: &Option<StringArray>| {
                        a.as_ref().filter(|a| !a.is_null(i)).map(|a| a.value(i).to_string())
                    };
                    out.push(MemoryHit {
                        namespace: get(&ns).unwrap_or_default(),
                        id: get(&id).unwrap_or_default(),
                        content: get(&content).unwrap_or_default(),
                        metadata: get(&meta).and_then(|s| serde_json::from_str(&s).ok()),
                        agent_id: get(&agent),
                        created_at: created.as_ref().map(|a| a.value(i)).unwrap_or_default(),
                        distance: dist.as_ref().map(|a| a.value(i) as f64),
                    });
                }
            }
            Ok(out)
        }
    }
}
