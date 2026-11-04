//! `enact-db demo`: one command that starts the server, sets up a team through
//! the public authz API, launches two agents, and prints a link per human.
//!
//! Team (role `demo-team`, write access to the shared doc and its transcript):
//! `user:alice`, `user:bob` (humans), `agent:scribe` (writes), `agent:sketch` (draws).
//! `user:carol` is a read-only viewer. Every participant gets its own token,
//! so every edit, drawing and chat message is attributed and audited.

pub mod agent;
pub mod llm;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

#[derive(clap::Args, Debug, Clone)]
pub struct DemoArgs {
    #[arg(long, default_value = "127.0.0.1:9494")]
    pub listen: std::net::SocketAddr,
    #[arg(long, default_value = "./enact-db-demo")]
    pub data_dir: PathBuf,
    /// Shared document id
    #[arg(long, default_value = "demo")]
    pub doc: String,
    /// Agent typing / drawing speed (ms per step)
    #[arg(long, default_value_t = 35)]
    pub speed_ms: u64,
    /// Write tokens and links as JSON here (for scripts and tests)
    #[arg(long)]
    pub tokens_out: Option<PathBuf>,
    /// Don't start the built-in agents (run them yourself with `enact-db agent`)
    #[arg(long)]
    pub no_agents: bool,
    /// LLM model for the agents (OpenAI-compatible; default from OPENAI_COMPATIBLE_MODEL / MiniMax-M3)
    #[arg(long, env = "OPENAI_COMPATIBLE_MODEL")]
    pub model: Option<String>,
    /// Use scripted agents even if OPENAI_COMPATIBLE_* is configured
    #[arg(long)]
    pub no_llm: bool,
    /// Embedding model for agent long-term memory (OpenRouter; default nvidia/nemotron-3.5-embed-1b:free)
    #[arg(long, env = "OPENROUTER_EMBED_MODEL")]
    pub embed_model: Option<String>,
    /// Disable long-term memory (LanceDB); session history in AgentFS still applies
    #[arg(long)]
    pub no_memory: bool,
}

fn random_token() -> Result<String> {
    let mut raw = [0u8; 24];
    getrandom::fill(&mut raw).map_err(|e| anyhow::anyhow!("rng: {e}"))?;
    Ok(hex::encode(raw))
}

struct Admin {
    http: reqwest::Client,
    base: String,
    token: String,
}

impl Admin {
    async fn call(&self, method: reqwest::Method, path: &str, body: Value) -> Result<Value> {
        let r = self
            .http
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await?;
        let status = r.status();
        let v: Value = r.json().await.unwrap_or(Value::Null);
        if !status.is_success() && status.as_u16() != 409 {
            bail!("{path}: HTTP {status}: {v}");
        }
        Ok(v)
    }
    async fn grant(&self, principal: &str, resource: &str, privilege: &str) -> Result<()> {
        self.call(reqwest::Method::POST, "/v1/authz/grants", json!({"principal": principal, "resource": resource, "privilege": privilege}))
            .await
            .map(|_| ())
    }
    async fn token(&self, principal: &str) -> Result<String> {
        let v = self
            .call(reqwest::Method::POST, "/v1/authz/tokens", json!({"principal": principal, "name": "demo", "ttl_secs": 7 * 86400}))
            .await?;
        v["token"].as_str().map(str::to_string).context("no token in response")
    }
}

pub async fn run(args: DemoArgs) -> Result<()> {
    std::fs::create_dir_all(&args.data_dir)?;
    let admin_token = random_token()?;

    // Long-term agent memory: LanceDB, sized for the embedding model (probed once).
    let mut memory_note = "off (no OPENROUTER_API_KEY; agents keep session history in AgentFS)".to_string();
    let mut config = None;
    let embedder = if args.no_llm || args.no_memory || llm::Llm::from_env(args.model.clone()).is_none() {
        None
    } else {
        llm::Embedder::from_env(args.embed_model.clone())
    };
    if let Some(e) = &embedder {
        match e.dimensions().await {
            Ok(dims) if cfg!(feature = "lancedb") => {
                let slug: String = e.model.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '_' }).collect();
                let uri = std::fs::canonicalize(&args.data_dir)?.join("lancedb").join(slug);
                let path = args.data_dir.join("enact-db-demo.toml");
                std::fs::write(
                    &path,
                    format!(
                        "[memory]\nbackend = \"lancedb\"\nlancedb_uri = \"{}\"\ndimensions = {dims}\n",
                        uri.display().to_string().replace('\\', "/")
                    ),
                )?;
                config = Some(path);
                memory_note = format!("LanceDB ({} dims, {}) at {}", dims, e.model, uri.display());
            }
            Ok(dims) => {
                memory_note = format!("{} ({dims} dims) in the local Turso store (built without lancedb)", e.model);
            }
            Err(err) => {
                tracing::warn!(error = %err, "embedding model unavailable; long-term memory disabled");
                memory_note = format!("off (embedding model {} failed: {err})", e.model);
            }
        }
    }
    let serve_args = crate::ServeArgs {
        config,
        listen: Some(args.listen),
        data_dir: Some(args.data_dir.clone()),
        token: Some(admin_token.clone()),
        ..Default::default()
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(crate::serve(serve_args, Some(tx)));
    let addr = tokio::time::timeout(Duration::from_secs(30), rx).await.context("server did not start")??;
    let base = format!("http://{addr}");
    let admin = Admin { http: reqwest::Client::new(), base: base.clone(), token: admin_token.clone() };

    // Team, grants and tokens - all through the public authz API.
    let doc = args.doc.as_str();
    admin.call(reqwest::Method::POST, "/v1/authz/roles", json!({"id": "demo-team", "description": "humans and agents co-editing the demo"})).await?;
    for m in ["user:alice", "user:bob", "agent:scribe", "agent:sketch"] {
        admin.call(reqwest::Method::POST, "/v1/authz/roles/demo-team/members", json!({"member": m})).await?;
    }
    admin.grant("role:demo-team", &format!("doc:{doc}"), "write_data").await?;
    admin.grant("role:demo-team", &format!("transcript:{doc}"), "write_data").await?;
    admin.grant("role:demo-team", "table:gateway/system/agent_tool_calls", "select").await?;
    admin.grant("user:carol", &format!("doc:{doc}"), "select").await?;
    admin.grant("user:carol", &format!("transcript:{doc}"), "select").await?;
    admin.grant("user:carol", "table:gateway/system/agent_tool_calls", "select").await?;
    // each agent owns its own memory namespace
    for a in ["scribe", "sketch"] {
        admin.grant(&format!("agent:{a}"), &format!("memory:agent-{a}"), "ownership").await?;
    }

    let mut tokens = BTreeMap::new();
    for p in ["user:alice", "user:bob", "user:carol", "agent:scribe", "agent:sketch"] {
        tokens.insert(p.split_once(':').unwrap().1.to_string(), admin.token(p).await?);
    }
    let link = |who: &str| format!("{base}/demo?doc={doc}&token={}", tokens[who]);

    if !args.no_agents {
        for (name, persona) in [("scribe", "scribe"), ("sketch", "sketch")] {
            let a = agent::AgentArgs {
                server: base.clone(),
                token: tokens[name].clone(),
                name: name.into(),
                persona: persona.into(),
                doc: doc.into(),
                speed_ms: args.speed_ms,
                color: None,
                model: args.model.clone(),
                no_llm: args.no_llm,
                embed_model: args.embed_model.clone(),
                no_memory: args.no_memory || embedder.is_none(),
            };
            tokio::spawn(async move {
                if let Err(e) = agent::run(a).await {
                    tracing::error!(error = %e, "agent stopped");
                }
            });
        }
    }

    if let Some(p) = &args.tokens_out {
        let out = json!({
            "base": base, "doc": doc, "admin": admin_token, "tokens": tokens,
            "links": {"alice": link("alice"), "bob": link("bob"), "carol": link("carol")},
        });
        std::fs::write(p, serde_json::to_vec_pretty(&out)?)?;
    }

    println!(
        "\n  enact-db multiplayer demo is running on {base}\n\n  \
         Open each link in its own browser window:\n\n    \
         alice (editor):  {}\n    \
         bob   (editor):  {}\n    \
         carol (viewer):  {}\n\n  \
         Agents scribe (writes) and sketch (draws) are {}.\n  \
         Agent brain: {}\n  \
         Agent memory: session history in AgentFS (Turso); long-term {}\n  \
         In the chat try: @scribe summarize   @sketch box Risks   @sketch smile\n\n  \
         Admin token (break-glass): {admin_token}\n  Ctrl-C to stop.\n",
        link("alice"),
        link("bob"),
        link("carol"),
        if args.no_agents { "NOT started (--no-agents)" } else { "already in the document" },
        match (args.no_llm, llm::Llm::from_env(args.model.clone())) {
            (false, Some(l)) => format!("LLM {} (ask them anything with @scribe / @sketch)", l.model),
            _ => "scripted commands (set OPENAI_COMPATIBLE_BASE_URL / _API_KEY for an LLM)".to_string(),
        },
        memory_note
    );

    server.await??;
    Ok(())
}
