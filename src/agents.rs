//! Agent sandbox registry backed by real AgentFS databases (agentfs-sdk on Turso).
//!
//! Each agent gets `<data_dir>/agents/<agent_id>.db`, a standard AgentFS file
//! (POSIX-like filesystem, KV store, tool-call audit log). The `agentfs` CLI
//! can open these files directly (`agentfs fs ls <file>`, `agentfs timeline`).
//!
//! Only this process opens the files; agents reach them over HTTP, so there is
//! never cross-process SQLite lock contention.

use agentfs_sdk::error::Error as AfsError;
use agentfs_sdk::filesystem::{FsError, DEFAULT_FILE_MODE};
use agentfs_sdk::{AgentFS, AgentFSOptions, ToolCall};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, RwLock};

pub struct AgentRegistry {
    dir: PathBuf,
    open: RwLock<HashMap<String, Arc<AgentFS>>>,
    /// Serialises the open-or-create path so two first requests don't race.
    opening: Mutex<()>,
}

pub fn valid_agent_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

pub fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

impl AgentRegistry {
    pub fn new(data_dir: &std::path::Path) -> Result<Self> {
        let dir = data_dir.join("agents");
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir, open: RwLock::new(HashMap::new()), opening: Mutex::new(()) })
    }

    pub fn db_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.db"))
    }

    /// Get (opening or creating) the AgentFS for `id`.
    pub async fn get(&self, id: &str) -> Result<Arc<AgentFS>> {
        if !valid_agent_id(id) {
            return Err(anyhow!("invalid agent id '{id}' (allowed: A-Z a-z 0-9 - _)"));
        }
        if let Some(a) = self.open.read().await.get(id) {
            return Ok(a.clone());
        }
        let _guard = self.opening.lock().await;
        if let Some(a) = self.open.read().await.get(id) {
            return Ok(a.clone());
        }
        let path = self.db_path(id);
        let afs = AgentFS::open(AgentFSOptions::with_path(path.to_string_lossy().to_string())).await?;
        let afs = Arc::new(afs);
        self.open.write().await.insert(id.to_string(), afs.clone());
        tracing::info!(agent = id, path = %path.display(), "opened AgentFS");
        Ok(afs)
    }

    pub async fn is_open(&self, id: &str) -> bool {
        self.open.read().await.contains_key(id)
    }

    pub async fn open_ids(&self) -> Vec<String> {
        self.open.read().await.keys().cloned().collect()
    }

    /// Close an agent's database (checkpointing it). Fails if a request is
    /// still using it, so a file is never swapped under a live connection.
    pub async fn close(&self, id: &str) -> Result<()> {
        let _guard = self.opening.lock().await;
        let mut open = self.open.write().await;
        if let Some(afs) = open.remove(id) {
            match Arc::try_unwrap(afs) {
                Ok(afs) => {
                    wal_checkpoint(&afs).await.ok();
                    drop(afs);
                }
                Err(still_used) => {
                    open.insert(id.to_string(), still_used);
                    return Err(anyhow!("agent '{id}' is busy; retry"));
                }
            }
        }
        Ok(())
    }

    /// Agent ids that exist on disk (whether or not they're open).
    pub fn list_ids(&self) -> Result<Vec<String>> {
        let mut ids = vec![];
        for entry in std::fs::read_dir(&self.dir)? {
            let p = entry?.path();
            if p.extension().and_then(|e| e.to_str()) == Some("db") {
                if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                    if valid_agent_id(stem) {
                        ids.push(stem.to_string());
                    }
                }
            }
        }
        ids.sort();
        Ok(ids)
    }

    /// Append an audit record to the agent's AgentFS `tool_calls` table.
    pub async fn audit(
        &self,
        agent: &AgentFS,
        name: &str,
        params: serde_json::Value,
        started: i64,
        outcome: std::result::Result<serde_json::Value, String>,
    ) {
        let (res, err) = match outcome {
            Ok(v) => (Some(v), None),
            Err(e) => (None, Some(e)),
        };
        if let Err(e) = agent.tools.record(name, started, now_secs(), Some(params), res, err.as_deref()).await {
            tracing::warn!(error = %e, tool = name, "failed to write audit record");
        }
    }

    /// Recent tool calls for every agent on disk (for the `agent_tool_calls` SQL table).
    pub async fn all_tool_calls(&self, per_agent_limit: i64) -> Result<Vec<(String, ToolCall)>> {
        let mut out = vec![];
        for id in self.list_ids()? {
            let a = self.get(&id).await?;
            for tc in a.tools.recent(Some(per_agent_limit)).await? {
                out.push((id.clone(), tc));
            }
        }
        Ok(out)
    }

    /// All KV pairs for every agent on disk (for the `agent_kv` SQL table).
    pub async fn all_kv(&self) -> Result<Vec<(String, String, String)>> {
        let mut out = vec![];
        for id in self.list_ids()? {
            let a = self.get(&id).await?;
            for k in a.kv.keys().await? {
                if let Some(v) = a.kv.get::<serde_json::Value>(&k).await? {
                    out.push((id.clone(), k, v.to_string()));
                }
            }
        }
        Ok(out)
    }
}

/// Fold the WAL into the main database file so the `.db` is self-contained
/// (required before copying it to the blob tier).
pub async fn wal_checkpoint(afs: &AgentFS) -> Result<()> {
    let pooled = afs.get_connection().await?;
    let mut rows = pooled.connection().query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;
    while rows.next().await?.is_some() {}
    Ok(())
}

// ---- filesystem helpers -------------------------------------------------------

pub fn normalize(path: &str) -> Result<String> {
    let mut parts = vec![];
    for c in path.split('/') {
        match c {
            "" | "." => {}
            ".." => return Err(anyhow!("'..' is not allowed in agent paths")),
            c => parts.push(c),
        }
    }
    Ok(format!("/{}", parts.join("/")))
}

/// `mkdir -p` for the parents of `path`.
pub async fn ensure_parents(afs: &AgentFS, path: &str) -> Result<()> {
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    let mut cur = String::new();
    for p in &parts[..parts.len().saturating_sub(1)] {
        cur.push('/');
        cur.push_str(p);
        match afs.fs.stat(&cur).await? {
            Some(st) if st.is_directory() => {}
            Some(_) => return Err(anyhow!("{cur} exists and is not a directory")),
            None => afs.fs.mkdir(&cur, 0, 0).await?,
        }
    }
    Ok(())
}

/// Replace the whole content of a file, creating it (and parents) if needed.
pub async fn write_file(afs: &AgentFS, path: &str, data: &[u8]) -> Result<()> {
    ensure_parents(afs, path).await?;
    match afs.fs.stat(path).await? {
        Some(st) if st.is_directory() => return Err(FsError::IsADirectory.into()),
        Some(_) => afs.fs.truncate(path, 0).await?,
        None => {
            afs.fs.create_file(path, DEFAULT_FILE_MODE, 0, 0).await?;
        }
    }
    if !data.is_empty() {
        afs.fs.pwrite(path, 0, data).await?;
    }
    Ok(())
}

/// Map an AgentFS error to an HTTP-ish status code.
pub fn status_for(err: &anyhow::Error) -> u16 {
    if let Some(AfsError::Fs(fe)) = err.downcast_ref::<AfsError>() {
        return match fe {
            FsError::NotFound => 404,
            FsError::AlreadyExists | FsError::NotEmpty => 409,
            _ => 400,
        };
    }
    if let Some(fe) = err.downcast_ref::<FsError>() {
        return match fe {
            FsError::NotFound => 404,
            FsError::AlreadyExists | FsError::NotEmpty => 409,
            _ => 400,
        };
    }
    500
}
