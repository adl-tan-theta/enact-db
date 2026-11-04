//! enact-db – one binary for multiplayer (human + agent) AI workspaces.
//!
//! * DataFusion federation over Postgres / Snowflake / Azure Blob / local files
//! * AgentFS (Turso) sandboxes per agent: files, KV, tool-call audit
//! * Yjs-compatible CRDT hub over WebSocket for humans and agents
//! * Memory (Turso vectors | pgvector | LanceDB) and transcripts (→ DuckDB / Postgres)
//! * Blob tier (Azure) for sandbox sync and agent artifacts

mod agents;
mod authz;
mod blob;
mod catalog;
mod config;
mod crdt;
mod databricks;
mod demo;
#[cfg(feature = "iceberg")]
mod iceberg;
mod memory;
#[cfg(feature = "postgres")]
mod pg;
mod quack;
mod server;
mod store;
mod systables;
mod transcripts;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "enact-db", version, about, args_conflicts_with_subcommands = true)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
    #[command(flatten)]
    serve: ServeArgs,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run the server (default).
    Serve(ServeArgs),
    /// Run an agent that joins a shared document as a live CRDT peer.
    Agent(demo::agent::AgentArgs),
    /// One-command multiplayer demo: server + team + tokens + two agents.
    Demo(demo::DemoArgs),
}

#[derive(clap::Args, Debug, Clone, Default)]
pub struct ServeArgs {
    /// Path to enact-db.toml
    #[arg(short, long, env = "ENACT_DB_CONFIG")]
    pub config: Option<PathBuf>,
    /// Override [server] listen address
    #[arg(long, env = "ENACT_DB_LISTEN")]
    pub listen: Option<std::net::SocketAddr>,
    /// Override [server] data_dir
    #[arg(long, env = "ENACT_DB_DATA_DIR")]
    pub data_dir: Option<PathBuf>,
    /// Override [server] auth_token (break-glass admin token)
    #[arg(long, env = "ENACT_DB_TOKEN")]
    pub token: Option<String>,
    /// Pull these agents from the blob tier before serving (cloud sandbox boot).
    #[arg(long = "pull-agent")]
    pub pull_agents: Vec<String>,
}

/// Sections that live outside `config::Config` (kept in their own modules).
#[derive(Deserialize, Default)]
struct Extra {
    #[serde(default)]
    memory: memory::MemoryConfig,
    #[serde(default)]
    transcripts: transcripts::TranscriptConfig,
    #[serde(default)]
    blob: blob::BlobConfig,
    #[serde(default)]
    auth: authz::AuthConfig,
    #[cfg(feature = "iceberg")]
    #[serde(default)]
    iceberg: Vec<iceberg::IcebergConfig>,
}



#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "enact_db=info,warn".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        None => serve(cli.serve, None).await,
        Some(Cmd::Serve(a)) => serve(a, None).await,
        Some(Cmd::Agent(a)) => demo::agent::run(a).await,
        Some(Cmd::Demo(d)) => demo::run(d).await,
    }
}

/// Run the server until Ctrl-C. `ready` receives the bound address once listening.
pub async fn serve(args: ServeArgs, ready: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>) -> Result<()> {
    let mut cfg = config::Config::load(args.config.as_deref())?;
    let extra: Extra = match &args.config {
        Some(p) => toml::from_str(&std::fs::read_to_string(p)?).context("parsing config")?,
        None => Extra::default(),
    };
    if let Some(l) = args.listen {
        cfg.server.listen = l;
    }
    if let Some(d) = args.data_dir {
        cfg.server.data_dir = d;
    }
    if args.token.is_some() {
        cfg.server.auth_token = args.token;
    }
    let data_dir = cfg.server.data_dir.clone();
    std::fs::create_dir_all(&data_dir)?;

    let store = store::HubStore::open(&data_dir, cfg.crdt.compact_after).await?;
    let agents = Arc::new(agents::AgentRegistry::new(&data_dir)?);
    let hub = Arc::new(crdt::Hub::new(store.clone()));
    let memory = Arc::new(memory::MemoryStore::open(&extra.memory, store.clone()).await?);
    let sinks = transcripts::start(&extra.transcripts, store.clone())?;
    let blob = blob::Blob::open(&extra.blob)?;

    if let Some(b) = &blob {
        for id in &args.pull_agents {
            b.pull_agent(&agents, id).await.with_context(|| format!("pulling agent {id}"))?;
        }
        b.start_auto_push(agents.clone(), extra.blob.push_interval_secs);
    } else if !args.pull_agents.is_empty() {
        anyhow::bail!("--pull-agent requires a [blob] url");
    }

    // [server] auth_token / --token is the break-glass admin token unless [auth] sets one.
    let mut auth_cfg = extra.auth.clone();
    if auth_cfg.admin_token.is_none() {
        auth_cfg.admin_token = cfg.server.auth_token.clone();
    }
    let authz = authz::Authz::open_store(&data_dir, auth_cfg).await?;
    if authz.open && !cfg.server.listen.ip().is_loopback() {
        anyhow::bail!(
            "refusing to listen on {} without authentication: set [auth] admin_token (or --token / \
             ENACT_DB_TOKEN) and/or [auth.oidc], or listen on 127.0.0.1",
            cfg.server.listen
        );
    }

    let ctx = catalog::build_context(&cfg.sources).await?;
    systables::register(&ctx, agents.clone(), hub.clone(), store.clone(), authz.clone())?;
    #[allow(unused_mut)]
    let mut sources: Vec<String> = cfg.sources.iter().map(|s| s.name().to_string()).collect();
    #[cfg(feature = "iceberg")]
    for name in iceberg::register_all(&ctx, &extra.iceberg).await? {
        sources.push(format!("{name} (iceberg)"));
    }

    let quack = Arc::new(quack::QuackServer::new(ctx.clone(), authz.clone()));

    let state = Arc::new(server::AppState {
        quack,
        authz: authz.clone(),
        ctx,
        agents: agents.clone(),
        hub,
        store,
        memory,
        blob: blob.clone(),
        sources,
        sinks,
        cfg: cfg.server.clone(),
        peer_seq: AtomicU64::new(1),
    });

    let listener = tokio::net::TcpListener::bind(cfg.server.listen).await?;
    if let Some(r) = ready {
        let _ = r.send(listener.local_addr()?);
    }
    tracing::info!(
        addr = %listener.local_addr()?,
        data_dir = %data_dir.display(),
        memory = state.memory.backend_name(),
        auth = if authz.open { "open" } else { "enforced" },
        "enact-db listening"
    );
    axum::serve(listener, server::router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    // Final sync of open agent sandboxes to the blob tier.
    if let Some(b) = &blob {
        for id in agents.open_ids().await {
            if let Err(e) = b.push_agent(&agents, &id).await {
                tracing::warn!(agent = %id, error = %e, "final push failed");
            }
        }
    }
    tracing::info!("shutdown complete");
    Ok(())
}
