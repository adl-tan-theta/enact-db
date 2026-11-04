//! Permissions: principals, roles, grants-as-data, and an audit log.
//!
//! The model follows Lakekeeper's (Apache-2.0) OpenFGA authorization model,
//! adapted to enact-db's resources. The implementation is enact-db's own:
//!
//! * **Principals**: `user:<id>`, `agent:<id>`, `role:<id>`. Roles are assigned
//!   to users, agents or other roles (transitively). Roles named `role:oidc:*`
//!   come from the identity provider's token and cannot be edited here.
//! * **Resources** form a tree; a grant on a node applies to its whole subtree:
//!   ```text
//!   server ─┬─ catalog:<c> ── schema:<c>/<s> ── table:<c>/<s>/<t>
//!           ├─ docs ── doc:<id>
//!           ├─ agents ── agent:<id>
//!           ├─ memories ── memory:<namespace>
//!           ├─ transcripts ── transcript:<session>
//!           ├─ blob ── blob:<dir> ── blob:<dir>/<file>
//!           └─ roles ── role:<id>
//!   ```
//! * **Privileges** (granted): `ownership`, `modify`, `write_data`, `select`,
//!   `describe`, `create`, `manage_grants`, `pass_grants`, `assume`. Effective
//!   privileges: `ownership ⇒ everything`, `modify ⇒ write_data ⇒ select ⇒ describe`,
//!   `create ⇒ describe`. `assume` on `agent:<x>` lets a user or service act as that agent.
//! * **Granting**: `manage_grants`, or holding the privilege plus `pass_grants`
//!   (Lakekeeper's `can_grant_*`); ownership/manage/pass grants need `manage_grants`.
//! * **Ownership on create**: creating a doc, memory namespace, transcript
//!   session or agent sandbox (with `create` on its container) makes the
//!   creator its owner. An agent always owns its own sandbox.
//! * **Listing**: a resource is listed when the caller can `describe` it or holds
//!   any grant below it (Lakekeeper's `visible_below`).
//! * **Audit**: every decision (allow and deny) and every permission change is
//!   appended to a versioned audit log with caller, acting identity, user agent,
//!   source and the grant that determined the outcome.

pub mod api;
pub mod authn;

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, Mutex};
use turso::Connection;

pub const AUDIT_VERSION: i64 = 1;
pub const PROVIDER_ROLE_PREFIX: &str = "role:oidc:";

// ---- privileges ------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Privilege {
    Describe,
    Select,
    WriteData,
    Modify,
    Create,
    Ownership,
    ManageGrants,
    PassGrants,
    Assume,
}

impl Privilege {
    pub const ALL: [Privilege; 9] = [
        Privilege::Describe,
        Privilege::Select,
        Privilege::WriteData,
        Privilege::Modify,
        Privilege::Create,
        Privilege::Ownership,
        Privilege::ManageGrants,
        Privilege::PassGrants,
        Privilege::Assume,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Privilege::Describe => "describe",
            Privilege::Select => "select",
            Privilege::WriteData => "write_data",
            Privilege::Modify => "modify",
            Privilege::Create => "create",
            Privilege::Ownership => "ownership",
            Privilege::ManageGrants => "manage_grants",
            Privilege::PassGrants => "pass_grants",
            Privilege::Assume => "assume",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Privilege::ALL
            .into_iter()
            .find(|p| p.as_str() == s)
            .ok_or_else(|| anyhow!("unknown privilege '{s}'"))
    }

    /// Does holding `self` grant `needed`?
    pub fn implies(self, needed: Privilege) -> bool {
        use Privilege::*;
        self == needed
            || match self {
                Ownership => true,
                Modify => matches!(needed, WriteData | Select | Describe),
                WriteData => matches!(needed, Select | Describe),
                Select => needed == Describe,
                Create => needed == Describe,
                ManageGrants => needed == Describe,
                _ => false,
            }
    }
}

// ---- resources & principals ----------------------------------------------------------

pub const CONTAINERS: [&str; 6] = ["docs", "agents", "memories", "transcripts", "blob", "roles"];

/// Parent of a resource in the tree (None for `server`).
pub fn parent(res: &str) -> Option<String> {
    if res == "server" {
        return None;
    }
    if CONTAINERS.contains(&res) {
        return Some("server".into());
    }
    let (kind, id) = res.split_once(':')?;
    Some(match kind {
        "catalog" => "server".into(),
        "schema" => format!("catalog:{}", id.split_once('/')?.0),
        "table" => format!("schema:{}", &id[..id.rfind('/')?]),
        "doc" => "docs".into(),
        "agent" => "agents".into(),
        "memory" => "memories".into(),
        "transcript" => "transcripts".into(),
        "role" => "roles".into(),
        "blob" => match id.rfind('/') {
            Some(i) => format!("blob:{}", &id[..i]),
            None => "blob".into(),
        },
        _ => return None,
    })
}

/// The resource and all its ancestors, nearest first.
pub fn lineage(res: &str) -> Vec<String> {
    let mut out = vec![res.to_string()];
    let mut cur = res.to_string();
    while let Some(p) = parent(&cur) {
        out.push(p.clone());
        cur = p;
    }
    out
}

pub fn validate_resource(res: &str) -> Result<()> {
    if res == "server" || CONTAINERS.contains(&res) {
        return Ok(());
    }
    let (kind, id) = res.split_once(':').ok_or_else(|| anyhow!("invalid resource '{res}'"))?;
    if id.is_empty() || id.contains(char::is_control) {
        bail!("invalid resource '{res}'");
    }
    let parts = id.split('/').count();
    let ok = match kind {
        "catalog" | "doc" | "agent" | "memory" | "transcript" | "role" => !id.contains('/'),
        "schema" => parts == 2,
        "table" => parts == 3,
        "blob" => !id.split('/').any(|p| p.is_empty() || p == ".."),
        _ => false,
    };
    if !ok {
        bail!("invalid resource '{res}' (see the resource tree in the docs)");
    }
    Ok(())
}

pub fn validate_principal(p: &str) -> Result<()> {
    let (kind, id) = p.split_once(':').ok_or_else(|| anyhow!("invalid principal '{p}'"))?;
    if !matches!(kind, "user" | "agent" | "role") || id.is_empty() || id.len() > 256 || id.contains(char::is_control) {
        bail!("invalid principal '{p}' (expected user:<id>, agent:<id> or role:<id>)");
    }
    Ok(())
}

pub fn table_resource(catalog: &str, schema: &str, table: &str) -> String {
    format!("table:{catalog}/{schema}/{table}")
}

/// Every (privilege, resource) a DataFusion plan needs: `select` on each table
/// it scans (subqueries included), `write_data` on DML targets, and
/// `describe` on the catalog for `information_schema` (it reveals table names).
pub fn plan_resources(plan: &datafusion::logical_expr::LogicalPlan) -> Vec<(Privilege, String)> {
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::logical_expr::LogicalPlan;
    let mut out: Vec<(Privilege, String)> = vec![];
    let mut add = |p: Privilege, r: String| {
        if !out.iter().any(|(q, s)| *q == p && *s == r) {
            out.push((p, r));
        }
    };
    let resolve = |t: &datafusion::common::TableReference| t.clone().resolve("gateway", "public");
    let _ = plan.apply_with_subqueries(|node| {
        match node {
            LogicalPlan::TableScan(ts) => {
                let r = resolve(&ts.table_name);
                if r.schema.as_ref() == "information_schema" {
                    add(Privilege::Describe, format!("catalog:{}", r.catalog));
                } else {
                    add(Privilege::Select, table_resource(&r.catalog, &r.schema, &r.table));
                }
            }
            LogicalPlan::Dml(d) => {
                let r = resolve(&d.table_name);
                add(Privilege::WriteData, table_resource(&r.catalog, &r.schema, &r.table));
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    });
    out
}

// ---- caller -------------------------------------------------------------------------

/// The authenticated identity behind a request.
#[derive(Debug, Clone, Serialize)]
pub struct Caller {
    /// Effective principal (after `X-Agent-Id` assumption).
    pub principal: String,
    /// The authenticated principal when acting as an agent.
    pub acting_for: Option<String>,
    /// Roles asserted by the identity provider for this request (provider-managed).
    pub token_roles: Vec<String>,
    /// Break-glass admin (bootstrap token): bypasses checks, always audited.
    pub break_glass: bool,
    #[serde(skip)]
    pub user_agent: Option<String>,
    pub source: &'static str,
}

impl Caller {
    pub fn agent_id(&self) -> Option<&str> {
        self.principal.strip_prefix("agent:")
    }
    /// CRDT origin / presence label.
    pub fn origin(&self) -> String {
        match self.principal.split_once(':') {
            Some(("agent", a)) => format!("agent:{a}"),
            Some((_, u)) => format!("human:{u}"),
            None => self.principal.clone(),
        }
    }
}

// ---- decisions ----------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct Decision {
    pub allowed: bool,
    pub determined_by: Option<String>,
}

impl Decision {
    fn allow(by: impl Into<String>) -> Self {
        Self { allowed: true, determined_by: Some(by.into()) }
    }
    fn deny() -> Self {
        Self { allowed: false, determined_by: None }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grant {
    pub id: i64,
    pub principal: String,
    pub resource: String,
    pub privilege: Privilege,
    pub granted_by: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RoleInfo {
    pub id: String,
    pub description: Option<String>,
    pub members: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TokenInfo {
    pub id: i64,
    pub principal: String,
    pub name: String,
    pub expires_at: Option<i64>,
    pub revoked: bool,
}

#[derive(Default)]
struct State {
    grants: HashMap<String, Vec<Grant>>,
    /// role -> direct members
    members: HashMap<String, HashSet<String>>,
    roles: HashMap<String, Option<String>>,
    /// sha256(token) hex -> token
    tokens: HashMap<String, TokenInfo>,
}

impl State {
    /// The principal plus every role it reaches (transitively).
    fn principal_set(&self, principal: &str, token_roles: &[String]) -> HashSet<String> {
        let mut set: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        queue.push_back(principal.to_string());
        for r in token_roles {
            queue.push_back(r.clone());
        }
        while let Some(p) = queue.pop_front() {
            if !set.insert(p.clone()) {
                continue;
            }
            for (role, members) in &self.members {
                if members.contains(&p) && !set.contains(role) {
                    queue.push_back(role.clone());
                }
            }
        }
        set
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditEvent {
    pub version: i64,
    pub ts_ms: i64,
    pub principal: String,
    pub acting_for: Option<String>,
    pub source: String,
    pub action: String,
    pub resource: String,
    pub decision: String,
    pub determined_by: Option<String>,
    pub reason: Option<String>,
    pub user_agent: Option<String>,
    pub detail: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AuthConfig {
    /// Break-glass bootstrap token (principal `user:admin`, bypasses checks, always audited).
    pub admin_token: Option<String>,
    /// Allow unauthenticated requests as `user:anonymous` (subject to grants).
    #[serde(default)]
    pub allow_anonymous: bool,
    #[serde(default)]
    pub oidc: Option<authn::OidcConfig>,
    /// Declarative grants applied at startup (granted_by = "config").
    #[serde(default)]
    pub grants: Vec<ConfigGrant>,
    /// Declarative roles applied at startup.
    #[serde(default)]
    pub roles: Vec<ConfigRole>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigGrant {
    pub principal: String,
    pub resource: String,
    pub privilege: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigRole {
    pub id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub members: Vec<String>,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS grants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    principal TEXT NOT NULL,
    resource TEXT NOT NULL,
    privilege TEXT NOT NULL,
    granted_by TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS roles (
    id TEXT PRIMARY KEY,
    description TEXT,
    created_by TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS role_members (
    role TEXT NOT NULL,
    member TEXT NOT NULL,
    added_by TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hash TEXT NOT NULL,
    principal TEXT NOT NULL,
    name TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER,
    revoked INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL,
    ts_ms INTEGER NOT NULL,
    principal TEXT NOT NULL,
    acting_for TEXT,
    source TEXT NOT NULL,
    action TEXT NOT NULL,
    resource TEXT NOT NULL,
    decision TEXT NOT NULL,
    determined_by TEXT,
    reason TEXT,
    user_agent TEXT,
    detail TEXT
);
"#;

enum AuditMsg {
    Event(AuditEvent),
    Flush(tokio::sync::oneshot::Sender<()>),
}

pub struct Authz {
    /// Open mode: no authentication configured; every request is allowed (dev).
    pub open: bool,
    pub cfg: AuthConfig,
    conn: Mutex<Connection>,
    state: RwLock<State>,
    audit_tx: mpsc::UnboundedSender<AuditMsg>,
    pub oidc: Option<authn::Oidc>,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[derive(Debug)]
pub struct Denied {
    pub status: u16,
    pub message: String,
}

impl std::fmt::Display for Denied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl Authz {
    pub async fn open_store(data_dir: &Path, cfg: AuthConfig) -> Result<Arc<Self>> {
        let path = data_dir.join("authz.db");
        let db = turso::Builder::new_local(&path.to_string_lossy()).build().await?;
        let conn = db.connect()?;
        conn.execute_batch(SCHEMA).await?;
        let open = cfg.admin_token.is_none() && cfg.oidc.is_none();
        let oidc = match &cfg.oidc {
            Some(o) => Some(authn::Oidc::new(o.clone())?),
            None => None,
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let me = Arc::new(Self { open, cfg: cfg.clone(), conn: Mutex::new(conn), state: RwLock::new(State::default()), audit_tx: tx, oidc });
        me.reload().await?;
        me.apply_config().await?;
        tokio::spawn(Self::audit_writer(me.clone(), rx));
        if open {
            tracing::warn!("authorization is OPEN (no [auth] admin_token / oidc configured): every request is allowed");
        }
        Ok(me)
    }

    async fn reload(&self) -> Result<()> {
        let conn = self.conn.lock().await;
        let mut st = State::default();
        let mut rows = conn.query("SELECT id, principal, resource, privilege, granted_by, created_at FROM grants", ()).await?;
        while let Some(r) = rows.next().await? {
            let g = Grant {
                id: r.get(0)?,
                principal: r.get(1)?,
                resource: r.get(2)?,
                privilege: Privilege::parse(&r.get::<String>(3)?)?,
                granted_by: r.get(4)?,
                created_at: r.get(5)?,
            };
            st.grants.entry(g.resource.clone()).or_default().push(g);
        }
        drop(rows);
        let mut rows = conn.query("SELECT id, description FROM roles", ()).await?;
        while let Some(r) = rows.next().await? {
            st.roles.insert(r.get(0)?, r.get(1)?);
        }
        drop(rows);
        let mut rows = conn.query("SELECT role, member FROM role_members", ()).await?;
        while let Some(r) = rows.next().await? {
            st.members.entry(r.get(0)?).or_default().insert(r.get(1)?);
        }
        drop(rows);
        let mut rows = conn
            .query("SELECT id, hash, principal, name, expires_at, revoked FROM tokens", ())
            .await?;
        while let Some(r) = rows.next().await? {
            let hash: String = r.get(1)?;
            st.tokens.insert(
                hash,
                TokenInfo {
                    id: r.get(0)?,
                    principal: r.get(2)?,
                    name: r.get(3)?,
                    expires_at: r.get(4)?,
                    revoked: r.get::<i64>(5)? != 0,
                },
            );
        }
        drop(rows);
        *self.state.write().unwrap() = st;
        Ok(())
    }

    async fn apply_config(&self) -> Result<()> {
        for r in self.cfg.roles.clone() {
            let id = if r.id.starts_with("role:") { r.id.clone() } else { format!("role:{}", r.id) };
            self.upsert_role(&id, r.description.as_deref(), "config").await?;
            for m in &r.members {
                validate_principal(m)?;
                self.add_member_raw(&id, m, "config").await?;
            }
        }
        for g in self.cfg.grants.clone() {
            validate_principal(&g.principal)?;
            validate_resource(&g.resource)?;
            self.insert_grant(&g.principal, &g.resource, Privilege::parse(&g.privilege)?, "config").await?;
        }
        Ok(())
    }

    // ---- decisions ----------------------------------------------------------------

    /// Evaluate without auditing.
    pub fn decide(&self, caller: &Caller, need: Privilege, res: &str) -> Decision {
        if self.open {
            return Decision::allow("open-mode");
        }
        if caller.break_glass {
            return Decision::allow("break-glass");
        }
        // An agent owns its own sandbox.
        if let Some(a) = caller.agent_id() {
            if res == format!("agent:{a}") {
                return Decision::allow("self");
            }
        }
        let st = self.state.read().unwrap();
        let set = st.principal_set(&caller.principal, &caller.token_roles);
        for r in lineage(res) {
            if let Some(gs) = st.grants.get(&r) {
                for g in gs {
                    if set.contains(&g.principal) && g.privilege.implies(need) {
                        return Decision::allow(format!(
                            "grant#{} {} {} on {}",
                            g.id,
                            g.principal,
                            g.privilege.as_str(),
                            g.resource
                        ));
                    }
                }
            }
        }
        Decision::deny()
    }

    /// Evaluate, audit, and turn a deny into an error.
    pub fn require(&self, caller: &Caller, need: Privilege, res: &str) -> Result<Decision, Denied> {
        let d = self.decide(caller, need, res);
        self.audit_decision(caller, need.as_str(), res, &d, None);
        if d.allowed {
            Ok(d)
        } else {
            Err(Denied {
                status: 403,
                message: format!("{} lacks '{}' on '{res}'", caller.principal, need.as_str()),
            })
        }
    }

    /// Access to a resource that may not exist yet. If it exists (or already has
    /// grants) `need` is required; otherwise `create` on the container is
    /// required and the caller becomes the new resource's owner.
    pub async fn require_or_create(
        &self,
        caller: &Caller,
        need: Privilege,
        res: &str,
        container: &str,
        exists: bool,
    ) -> Result<Decision, Denied> {
        let has_grants = self.state.read().unwrap().grants.contains_key(res);
        if exists || has_grants || self.open || caller.break_glass {
            return self.require(caller, need, res);
        }
        if let Some(a) = caller.agent_id() {
            if res == format!("agent:{a}") {
                return self.require(caller, need, res);
            }
        }
        let d = self.require(caller, Privilege::Create, container)?;
        self.insert_grant(&caller.principal, res, Privilege::Ownership, &caller.principal)
            .await
            .map_err(|e| Denied { status: 500, message: e.to_string() })?;
        self.audit(AuditEvent {
            action: "ownership.record".into(),
            resource: res.into(),
            decision: "allow".into(),
            determined_by: d.determined_by.clone(),
            ..self.event(caller)
        });
        Ok(d)
    }

    /// Lakekeeper-style list visibility: describe, or any grant at/below `res`.
    pub fn visible(&self, caller: &Caller, res: &str) -> bool {
        if self.decide(caller, Privilege::Describe, res).allowed {
            return true;
        }
        let st = self.state.read().unwrap();
        let set = st.principal_set(&caller.principal, &caller.token_roles);
        st.grants.iter().any(|(r, gs)| lineage(r).iter().any(|a| a == res) && gs.iter().any(|g| set.contains(&g.principal)))
    }

    pub fn can_grant(&self, caller: &Caller, p: Privilege, res: &str) -> bool {
        if self.decide(caller, Privilege::ManageGrants, res).allowed {
            return true;
        }
        if matches!(p, Privilege::Ownership | Privilege::ManageGrants | Privilege::PassGrants) {
            return false;
        }
        self.decide(caller, p, res).allowed && self.decide(caller, Privilege::PassGrants, res).allowed
    }

    // ---- audit ----------------------------------------------------------------------

    pub fn event(&self, caller: &Caller) -> AuditEvent {
        AuditEvent {
            version: AUDIT_VERSION,
            ts_ms: now_ms(),
            principal: caller.principal.clone(),
            acting_for: caller.acting_for.clone(),
            source: caller.source.to_string(),
            action: String::new(),
            resource: String::new(),
            decision: String::new(),
            determined_by: None,
            reason: None,
            user_agent: caller.user_agent.clone(),
            detail: None,
        }
    }

    fn audit_decision(&self, caller: &Caller, action: &str, res: &str, d: &Decision, reason: Option<String>) {
        self.audit(AuditEvent {
            action: action.into(),
            resource: res.into(),
            decision: if d.allowed { "allow" } else { "deny" }.into(),
            determined_by: d.determined_by.clone(),
            reason,
            ..self.event(caller)
        });
    }

    pub fn audit(&self, ev: AuditEvent) {
        let _ = self.audit_tx.send(AuditMsg::Event(ev));
    }

    async fn audit_writer(me: Arc<Self>, mut rx: mpsc::UnboundedReceiver<AuditMsg>) {
        while let Some(msg) = rx.recv().await {
            let ev = match msg {
                AuditMsg::Event(ev) => ev,
                AuditMsg::Flush(done) => {
                    let _ = done.send(());
                    continue;
                }
            };
            let conn = me.conn.lock().await;
            let r = conn
                .execute(
                    "INSERT INTO audit_log (version, ts_ms, principal, acting_for, source, action, resource, decision, determined_by, reason, user_agent, detail)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    (
                        ev.version,
                        ev.ts_ms,
                        ev.principal.as_str(),
                        ev.acting_for.clone(),
                        ev.source.as_str(),
                        ev.action.as_str(),
                        ev.resource.as_str(),
                        ev.decision.as_str(),
                        ev.determined_by.clone(),
                        ev.reason.clone(),
                        ev.user_agent.clone(),
                        ev.detail.as_ref().map(|d| d.to_string()),
                    ),
                )
                .await;
            if let Err(e) = r {
                tracing::error!(error = %e, "failed to write audit event");
            }
        }
    }

    pub async fn audit_events(&self, limit: i64) -> Result<Vec<AuditEvent>> {
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "SELECT version, ts_ms, principal, acting_for, source, action, resource, decision, determined_by, reason, user_agent, detail
                 FROM audit_log ORDER BY id DESC LIMIT ?1",
                (limit,),
            )
            .await?;
        let mut out = vec![];
        while let Some(r) = rows.next().await? {
            let detail: Option<String> = r.get(11)?;
            out.push(AuditEvent {
                version: r.get(0)?,
                ts_ms: r.get(1)?,
                principal: r.get(2)?,
                acting_for: r.get(3)?,
                source: r.get(4)?,
                action: r.get(5)?,
                resource: r.get(6)?,
                decision: r.get(7)?,
                determined_by: r.get(8)?,
                reason: r.get(9)?,
                user_agent: r.get(10)?,
                detail: detail.and_then(|d| serde_json::from_str(&d).ok()),
            });
        }
        Ok(out)
    }

    /// Wait until every audit event queued so far is written.
    pub async fn flush_audit(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.audit_tx.send(AuditMsg::Flush(tx)).is_ok() {
            let _ = rx.await;
        }
    }

    // ---- grants ---------------------------------------------------------------------

    pub async fn insert_grant(&self, principal: &str, res: &str, p: Privilege, by: &str) -> Result<Grant> {
        if let Some(g) = self
            .state
            .read()
            .unwrap()
            .grants
            .get(res)
            .and_then(|gs| gs.iter().find(|g| g.principal == principal && g.privilege == p).cloned())
        {
            return Ok(g);
        }
        let conn = self.conn.lock().await;
        let created_at = crate::agents::now_secs();
        let mut rows = conn
            .query(
                "INSERT INTO grants (principal, resource, privilege, granted_by, created_at) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING id",
                (principal, res, p.as_str(), by, created_at),
            )
            .await?;
        let id: i64 = rows.next().await?.context("no id")?.get(0)?;
        drop(rows);
        let g = Grant { id, principal: principal.into(), resource: res.into(), privilege: p, granted_by: by.into(), created_at };
        self.state.write().unwrap().grants.entry(res.into()).or_default().push(g.clone());
        Ok(g)
    }

    pub async fn delete_grant(&self, principal: &str, res: &str, p: Privilege) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn
            .execute(
                "DELETE FROM grants WHERE principal = ?1 AND resource = ?2 AND privilege = ?3",
                (principal, res, p.as_str()),
            )
            .await?;
        let mut st = self.state.write().unwrap();
        if let Some(gs) = st.grants.get_mut(res) {
            gs.retain(|g| !(g.principal == principal && g.privilege == p));
            if gs.is_empty() {
                st.grants.remove(res);
            }
        }
        Ok(n > 0)
    }

    /// Grants on `res` (and, with `subtree`, below it), optionally for one principal.
    pub fn list_grants(&self, res: Option<&str>, subtree: bool, principal: Option<&str>) -> Vec<Grant> {
        let st = self.state.read().unwrap();
        let mut out: Vec<Grant> = st
            .grants
            .iter()
            .filter(|(r, _)| match res {
                None => true,
                Some(res) => *r == res || (subtree && lineage(r).iter().any(|a| a == res)),
            })
            .flat_map(|(_, gs)| gs.iter().cloned())
            .filter(|g| principal.is_none_or(|p| g.principal == p))
            .collect();
        out.sort_by_key(|g| g.id);
        out
    }

    pub fn has_grants(&self, res: &str) -> bool {
        self.state.read().unwrap().grants.contains_key(res)
    }

    /// Effective privileges of the caller on a resource (for whoami / UIs).
    pub fn effective(&self, caller: &Caller, res: &str) -> Vec<&'static str> {
        Privilege::ALL.iter().filter(|p| self.decide(caller, **p, res).allowed).map(|p| p.as_str()).collect()
    }

    pub fn roles_of(&self, caller: &Caller) -> Vec<String> {
        let st = self.state.read().unwrap();
        let mut v: Vec<String> = st
            .principal_set(&caller.principal, &caller.token_roles)
            .into_iter()
            .filter(|p| p.starts_with("role:"))
            .collect();
        v.sort();
        v
    }

    // ---- roles ------------------------------------------------------------------------

    pub async fn upsert_role(&self, id: &str, description: Option<&str>, by: &str) -> Result<()> {
        validate_principal(id)?;
        if !id.starts_with("role:") {
            bail!("role ids start with 'role:'");
        }
        if self.state.read().unwrap().roles.contains_key(id) {
            return Ok(());
        }
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO roles (id, description, created_by, created_at) VALUES (?1, ?2, ?3, ?4)",
            (id, description, by, crate::agents::now_secs()),
        )
        .await?;
        self.state.write().unwrap().roles.insert(id.into(), description.map(String::from));
        Ok(())
    }

    pub async fn delete_role(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute("DELETE FROM roles WHERE id = ?1", (id,)).await?;
        conn.execute("DELETE FROM role_members WHERE role = ?1 OR member = ?1", (id,)).await?;
        conn.execute("DELETE FROM grants WHERE principal = ?1 OR resource = ?2", (id, id)).await?;
        drop(conn);
        let mut st = self.state.write().unwrap();
        st.roles.remove(id);
        st.members.remove(id);
        for m in st.members.values_mut() {
            m.remove(id);
        }
        for gs in st.grants.values_mut() {
            gs.retain(|g| g.principal != id);
        }
        st.grants.remove(id);
        st.grants.retain(|_, gs| !gs.is_empty());
        Ok(n > 0)
    }

    pub async fn add_member_raw(&self, role: &str, member: &str, by: &str) -> Result<()> {
        if self.state.read().unwrap().members.get(role).is_some_and(|m| m.contains(member)) {
            return Ok(());
        }
        if member == role {
            bail!("a role cannot contain itself");
        }
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO role_members (role, member, added_by, created_at) VALUES (?1, ?2, ?3, ?4)",
            (role, member, by, crate::agents::now_secs()),
        )
        .await?;
        self.state.write().unwrap().members.entry(role.into()).or_default().insert(member.into());
        Ok(())
    }

    pub async fn remove_member(&self, role: &str, member: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute("DELETE FROM role_members WHERE role = ?1 AND member = ?2", (role, member)).await?;
        if let Some(m) = self.state.write().unwrap().members.get_mut(role) {
            m.remove(member);
        }
        Ok(n > 0)
    }

    pub fn roles(&self) -> Vec<RoleInfo> {
        let st = self.state.read().unwrap();
        let mut v: Vec<RoleInfo> = st
            .roles
            .iter()
            .map(|(id, d)| {
                let mut members: Vec<String> = st.members.get(id).map(|m| m.iter().cloned().collect()).unwrap_or_default();
                members.sort();
                RoleInfo { id: id.clone(), description: d.clone(), members }
            })
            .collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        v
    }

    pub fn role_exists(&self, id: &str) -> bool {
        self.state.read().unwrap().roles.contains_key(id)
    }

    // ---- tokens ----------------------------------------------------------------------

    pub async fn create_token(&self, principal: &str, name: &str, ttl_secs: Option<i64>, by: &str) -> Result<(i64, String)> {
        validate_principal(principal)?;
        let mut raw = [0u8; 32];
        getrandom::fill(&mut raw).map_err(|e| anyhow!("rng: {e}"))?;
        let token = format!("edb_{}", hex::encode(raw));
        let hash = authn::hash_token(&token);
        let now = crate::agents::now_secs();
        let expires_at = ttl_secs.map(|t| now + t);
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "INSERT INTO tokens (hash, principal, name, created_by, created_at, expires_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING id",
                (hash.as_str(), principal, name, by, now, expires_at),
            )
            .await?;
        let id: i64 = rows.next().await?.context("no id")?.get(0)?;
        drop(rows);
        self.state.write().unwrap().tokens.insert(
            hash,
            TokenInfo { id, principal: principal.into(), name: name.into(), expires_at, revoked: false },
        );
        Ok((id, token))
    }

    pub fn lookup_token(&self, token: &str) -> Option<TokenInfo> {
        let hash = authn::hash_token(token);
        let st = self.state.read().unwrap();
        let t = st.tokens.get(&hash)?;
        if t.revoked || t.expires_at.is_some_and(|e| e < crate::agents::now_secs()) {
            return None;
        }
        Some(t.clone())
    }

    pub fn tokens(&self, principal: Option<&str>) -> Vec<TokenInfo> {
        let st = self.state.read().unwrap();
        let mut v: Vec<TokenInfo> =
            st.tokens.values().filter(|t| principal.is_none_or(|p| t.principal == p)).cloned().collect();
        v.sort_by_key(|t| t.id);
        v
    }

    pub async fn revoke_token(&self, id: i64) -> Result<Option<String>> {
        let conn = self.conn.lock().await;
        conn.execute("UPDATE tokens SET revoked = 1 WHERE id = ?1", (id,)).await?;
        let mut st = self.state.write().unwrap();
        for t in st.tokens.values_mut() {
            if t.id == id {
                t.revoked = true;
                return Ok(Some(t.principal.clone()));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tree() {
        assert_eq!(
            lineage("table:lake/sales/orders"),
            vec!["table:lake/sales/orders", "schema:lake/sales", "catalog:lake", "server"]
        );
        assert_eq!(lineage("blob:reports/q3/a.txt"), vec!["blob:reports/q3/a.txt", "blob:reports/q3", "blob:reports", "blob", "server"]);
        assert_eq!(lineage("doc:design"), vec!["doc:design", "docs", "server"]);
        assert!(validate_resource("table:a/b").is_err());
        assert!(validate_resource("blob:a/../b").is_err());
        assert!(validate_resource("schema:gateway/public").is_ok());
    }

    #[test]
    fn implication() {
        use Privilege::*;
        assert!(Ownership.implies(ManageGrants));
        assert!(Modify.implies(Select));
        assert!(WriteData.implies(Describe));
        assert!(!Select.implies(WriteData));
        assert!(!ManageGrants.implies(Select));
        assert!(!PassGrants.implies(Select));
    }
}
