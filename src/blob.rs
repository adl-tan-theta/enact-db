//! Blob tier (Azure Blob / ADLS, or any object_store URL such as file:// for tests).
//!
//! Two jobs:
//! 1. **Agent sandbox sync** – push an agent's AgentFS file (checkpointed, so it
//!    is a single self-contained SQLite file) to `<prefix>/agents/<id>.db`, and
//!    pull it back on another machine. A cloud sandbox boots the same binary,
//!    pulls the agent, works locally at SQLite speed, and pushes on exit/interval.
//! 2. **Artifact store** – agents write large outputs (reports, datasets,
//!    images) to `<prefix>/artifacts/<agent>/<path>` through the gateway, so
//!    sandboxes never hold cloud credentials themselves.

use crate::agents::AgentRegistry;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct BlobConfig {
    /// e.g. "az://mycontainer/enact-db" or "file:///var/lib/enact-db-blob"
    pub url: Option<String>,
    /// Push all open agents every N seconds (0 = only on demand / shutdown).
    #[serde(default)]
    pub push_interval_secs: u64,
}

pub struct Blob {
    store: Arc<dyn ObjectStore>,
    prefix: ObjPath,
    pub url: String,
}

impl Blob {
    pub fn open(cfg: &BlobConfig) -> Result<Option<Arc<Self>>> {
        let Some(url) = cfg.url.as_deref() else { return Ok(None) };
        let parsed = url::Url::parse(url).with_context(|| format!("invalid blob url {url}"))?;
        // Azure credentials come from the standard env vars
        // (AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY / SAS / managed identity, ...).
        let opts: Vec<(String, String)> = std::env::vars()
            .filter(|(k, _)| k.starts_with("AZURE_"))
            .map(|(k, v)| (k.to_ascii_lowercase(), v))
            .collect();
        if parsed.scheme() == "file" {
            std::fs::create_dir_all(parsed.path()).ok();
        }
        let (store, prefix) = object_store::parse_url_opts(&parsed, opts)
            .with_context(|| format!("opening object store {url}"))?;
        tracing::info!(url, "blob tier ready");
        Ok(Some(Arc::new(Self { store: Arc::from(store), prefix, url: url.to_string() })))
    }

    fn key(&self, rel: &str) -> ObjPath {
        let mut p = self.prefix.clone();
        for part in rel.split('/').filter(|s| !s.is_empty()) {
            p = p.clone().join(part);
        }
        p
    }

    pub async fn put(&self, rel: &str, data: Bytes) -> Result<()> {
        self.store.put(&self.key(rel), PutPayload::from_bytes(data)).await?;
        Ok(())
    }

    pub async fn get(&self, rel: &str) -> Result<Option<Bytes>> {
        match self.store.get(&self.key(rel)).await {
            Ok(r) => Ok(Some(r.bytes().await?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn list(&self, rel: &str) -> Result<Vec<(String, u64)>> {
        use futures::TryStreamExt;
        let base = self.key(rel);
        let items: Vec<_> = self.store.list(Some(&base)).try_collect().await?;
        let strip = format!("{}/", self.prefix.as_ref());
        Ok(items
            .into_iter()
            .map(|m| {
                let loc = m.location.as_ref();
                (loc.strip_prefix(&strip).unwrap_or(loc).to_string(), m.size)
            })
            .collect())
    }

    /// Checkpoint an agent's database and upload it.
    pub async fn push_agent(&self, agents: &AgentRegistry, id: &str) -> Result<u64> {
        let afs = agents.get(id).await?;
        crate::agents::wal_checkpoint(&afs).await.context("checkpointing AgentFS before upload")?;
        let path = agents.db_path(id);
        let wal = path.with_extension("db-wal");
        if tokio::fs::metadata(&wal).await.map(|m| m.len() > 0).unwrap_or(false) {
            return Err(anyhow!("WAL not empty after checkpoint (concurrent writer?); retry"));
        }
        let data = tokio::fs::read(&path).await.with_context(|| format!("reading {}", path.display()))?;
        let n = data.len() as u64;
        self.put(&format!("agents/{id}.db"), Bytes::from(data)).await?;
        tracing::info!(agent = id, bytes = n, "pushed agent to blob");
        Ok(n)
    }

    /// Download an agent's database. Refuses if the agent is open in this process
    /// (call `close` first) so we never swap a file under a live connection.
    pub async fn pull_agent(&self, agents: &AgentRegistry, id: &str) -> Result<u64> {
        if agents.is_open(id).await {
            agents.close(id).await?;
        }
        let data = self
            .get(&format!("agents/{id}.db"))
            .await?
            .ok_or_else(|| anyhow!("agent '{id}' not found in blob store"))?;
        let path = agents.db_path(id);
        let tmp = path.with_extension("db.download");
        tokio::fs::write(&tmp, &data).await?;
        for suffix in ["db-wal", "db-shm"] {
            let _ = tokio::fs::remove_file(path.with_extension(suffix)).await;
        }
        tokio::fs::rename(&tmp, &path).await?;
        tracing::info!(agent = id, bytes = data.len(), "pulled agent from blob");
        Ok(data.len() as u64)
    }

    pub fn start_auto_push(self: &Arc<Self>, agents: Arc<AgentRegistry>, every_secs: u64) {
        if every_secs == 0 {
            return;
        }
        let blob = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(every_secs));
            tick.tick().await;
            loop {
                tick.tick().await;
                for id in agents.open_ids().await {
                    if let Err(e) = blob.push_agent(&agents, &id).await {
                        tracing::warn!(agent = %id, error = %e, "auto-push failed");
                    }
                }
            }
        });
    }
}
