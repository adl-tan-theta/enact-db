//! `/v1/authz/*`: whoami, batch checks, grants, roles, tokens, audit; plus
//! the authentication middleware that turns every request into a [`Caller`].

use super::{authn, validate_principal, validate_resource, Caller, Denied, Privilege, PROVIDER_ROLE_PREFIX};
use crate::server::{ApiError, AppState};
use axum::extract::{FromRequestParts, Path, Query, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

type St = State<Arc<AppState>>;
type ApiResult<T> = Result<T, ApiError>;

impl From<Denied> for ApiError {
    fn from(d: Denied) -> Self {
        ApiError::new(StatusCode::from_u16(d.status).unwrap_or(StatusCode::FORBIDDEN), d.message)
    }
}

impl<S: Send + Sync> FromRequestParts<S> for Caller {
    type Rejection = ApiError;
    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Caller>()
            .cloned()
            .ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "no caller on request"))
    }
}

fn header<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name).and_then(|v| v.to_str().ok())
}

/// Authenticate every API request and attach the [`Caller`].
pub async fn middleware(State(st): St, mut req: Request, next: Next) -> Response {
    let headers = req.headers();
    let query = req.uri().query().map(str::to_string);
    let q = |k: &str| {
        query.as_deref().and_then(|q| url::form_urlencoded::parse(q.as_bytes()).find(|(n, _)| n == k).map(|(_, v)| v.into_owned()))
    };
    let token = authn::bearer(headers, query.as_deref());
    let ua = header(headers, "user-agent").map(str::to_string);
    let agent = header(headers, "x-agent-id").map(str::to_string).or_else(|| q("agent"));
    let actor = header(headers, "x-actor").map(str::to_string).or_else(|| q("user"));
    let source = if req.uri().path().starts_with("/ws/") { "ws" } else { "http" };
    let caller = match st.authz.authenticate(token.as_deref(), ua, source).await {
        Ok(c) => c,
        Err(d) => return ApiError::from(d).into_response(),
    };
    let caller = match st.authz.assume(caller, agent.as_deref(), actor.as_deref()) {
        Ok(c) => c,
        Err(d) => return ApiError::from(d).into_response(),
    };
    req.extensions_mut().insert(caller);
    next.run(req).await
}

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/authz/whoami", get(whoami))
        .route("/v1/authz/check", post(check))
        .route("/v1/authz/grants", get(list_grants).post(create_grant).delete(revoke_grant))
        .route("/v1/authz/roles", get(list_roles).post(create_role))
        .route("/v1/authz/roles/{id}", delete(delete_role))
        .route("/v1/authz/roles/{id}/members", post(add_member))
        .route("/v1/authz/roles/{id}/members/{member}", delete(remove_member))
        .route("/v1/authz/tokens", get(list_tokens).post(create_token))
        .route("/v1/authz/tokens/{id}", delete(revoke_token))
        .route("/v1/authz/audit", get(audit))
}

fn role_id(id: &str) -> String {
    if id.starts_with("role:") {
        id.to_string()
    } else {
        format!("role:{id}")
    }
}

async fn whoami(State(st): St, caller: Caller) -> Json<Value> {
    let az = &st.authz;
    Json(json!({
        "principal": caller.principal,
        "acting_for": caller.acting_for,
        "break_glass": caller.break_glass,
        "roles": az.roles_of(&caller),
        "open_mode": az.open,
        "server": az.effective(&caller, "server"),
    }))
}

#[derive(Deserialize)]
struct CheckItem {
    resource: String,
    privilege: String,
    #[serde(default)]
    principal: Option<String>,
}

#[derive(Deserialize)]
struct CheckReq {
    checks: Vec<CheckItem>,
}

/// Batch check, reporting which grant determined each result.
async fn check(State(st): St, caller: Caller, Json(req): Json<CheckReq>) -> ApiResult<Json<Value>> {
    let az = &st.authz;
    let mut out = vec![];
    for c in req.checks {
        validate_resource(&c.resource).map_err(|e| ApiError::bad(e.to_string()))?;
        let p = Privilege::parse(&c.privilege).map_err(|e| ApiError::bad(e.to_string()))?;
        let subject = match &c.principal {
            Some(other) if *other != caller.principal => {
                validate_principal(other).map_err(|e| ApiError::bad(e.to_string()))?;
                // checking someone else's access needs manage_grants on the resource
                az.require(&caller, Privilege::ManageGrants, &c.resource)?;
                Caller { principal: other.clone(), acting_for: None, token_roles: vec![], break_glass: false, user_agent: None, source: "check" }
            }
            _ => caller.clone(),
        };
        let d = az.decide(&subject, p, &c.resource);
        out.push(json!({
            "principal": subject.principal, "resource": c.resource, "privilege": p.as_str(),
            "allowed": d.allowed, "determined_by": d.determined_by,
        }));
    }
    Ok(Json(json!({ "results": out })))
}

#[derive(Deserialize)]
struct GrantQuery {
    resource: Option<String>,
    #[serde(default)]
    subtree: bool,
    principal: Option<String>,
}

async fn list_grants(State(st): St, caller: Caller, Query(q): Query<GrantQuery>) -> ApiResult<Json<Value>> {
    let az = &st.authz;
    let own = q.principal.as_deref() == Some(caller.principal.as_str());
    match &q.resource {
        Some(r) if !own => {
            az.require(&caller, Privilege::ManageGrants, r)?;
        }
        None if !own => {
            az.require(&caller, Privilege::ManageGrants, "server")?;
        }
        _ => {}
    }
    Ok(Json(json!({ "grants": az.list_grants(q.resource.as_deref(), q.subtree, q.principal.as_deref()) })))
}

#[derive(Deserialize)]
struct GrantReq {
    principal: String,
    resource: String,
    privilege: String,
}

async fn create_grant(State(st): St, caller: Caller, Json(g): Json<GrantReq>) -> ApiResult<Json<Value>> {
    let az = &st.authz;
    validate_principal(&g.principal).map_err(|e| ApiError::bad(e.to_string()))?;
    validate_resource(&g.resource).map_err(|e| ApiError::bad(e.to_string()))?;
    let p = Privilege::parse(&g.privilege).map_err(|e| ApiError::bad(e.to_string()))?;
    if p == Privilege::Assume && !g.resource.starts_with("agent:") && g.resource != "agents" {
        return Err(ApiError::bad("'assume' applies to agent:<id> or agents"));
    }
    let allowed = az.can_grant(&caller, p, &g.resource);
    az.audit(super::AuditEvent {
        action: "grant.create".into(),
        resource: g.resource.clone(),
        decision: if allowed { "allow" } else { "deny" }.into(),
        detail: Some(json!({"principal": g.principal, "privilege": p.as_str()})),
        ..az.event(&caller)
    });
    if !allowed {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            format!("{} may not grant '{}' on '{}'", caller.principal, p.as_str(), g.resource),
        ));
    }
    let grant = az.insert_grant(&g.principal, &g.resource, p, &caller.principal).await?;
    Ok(Json(json!({ "grant": grant })))
}

async fn revoke_grant(State(st): St, caller: Caller, Json(g): Json<GrantReq>) -> ApiResult<Json<Value>> {
    let az = &st.authz;
    let p = Privilege::parse(&g.privilege).map_err(|e| ApiError::bad(e.to_string()))?;
    az.require(&caller, Privilege::ManageGrants, &g.resource)?;
    let removed = az.delete_grant(&g.principal, &g.resource, p).await?;
    az.audit(super::AuditEvent {
        action: "grant.revoke".into(),
        resource: g.resource.clone(),
        decision: "allow".into(),
        detail: Some(json!({"principal": g.principal, "privilege": p.as_str(), "removed": removed})),
        ..az.event(&caller)
    });
    Ok(Json(json!({ "revoked": removed })))
}

async fn list_roles(State(st): St, caller: Caller) -> ApiResult<Json<Value>> {
    let az = &st.authz;
    let roles: Vec<_> = az.roles().into_iter().filter(|r| az.visible(&caller, &r.id) || az.visible(&caller, "roles")).collect();
    Ok(Json(json!({ "roles": roles })))
}

#[derive(Deserialize)]
struct RoleReq {
    id: String,
    #[serde(default)]
    description: Option<String>,
}

fn reject_provider_role(id: &str) -> ApiResult<()> {
    if id.starts_with(PROVIDER_ROLE_PREFIX) {
        return Err(ApiError::new(StatusCode::FORBIDDEN, format!("{id} is managed by the identity provider")));
    }
    Ok(())
}

async fn create_role(State(st): St, caller: Caller, Json(r): Json<RoleReq>) -> ApiResult<Json<Value>> {
    let az = &st.authz;
    let id = role_id(&r.id);
    reject_provider_role(&id)?;
    validate_principal(&id).map_err(|e| ApiError::bad(e.to_string()))?;
    if az.role_exists(&id) {
        return Err(ApiError::new(StatusCode::CONFLICT, format!("{id} exists")));
    }
    az.require(&caller, Privilege::Create, "roles")?;
    az.upsert_role(&id, r.description.as_deref(), &caller.principal).await?;
    az.insert_grant(&caller.principal, &id, Privilege::Ownership, &caller.principal).await?;
    az.audit(super::AuditEvent { action: "role.create".into(), resource: id.clone(), decision: "allow".into(), ..az.event(&caller) });
    Ok(Json(json!({ "role": id })))
}

async fn delete_role(State(st): St, caller: Caller, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let az = &st.authz;
    let id = role_id(&id);
    reject_provider_role(&id)?;
    az.require(&caller, Privilege::Ownership, &id)?;
    let removed = az.delete_role(&id).await?;
    az.audit(super::AuditEvent { action: "role.delete".into(), resource: id.clone(), decision: "allow".into(), ..az.event(&caller) });
    Ok(Json(json!({ "deleted": removed })))
}

#[derive(Deserialize)]
struct MemberReq {
    member: String,
}

async fn add_member(State(st): St, caller: Caller, Path(id): Path<String>, Json(m): Json<MemberReq>) -> ApiResult<Json<Value>> {
    let az = &st.authz;
    let id = role_id(&id);
    reject_provider_role(&id)?;
    validate_principal(&m.member).map_err(|e| ApiError::bad(e.to_string()))?;
    if !az.role_exists(&id) {
        return Err(ApiError::not_found(format!("{id} not found")));
    }
    az.require(&caller, Privilege::ManageGrants, &id)?;
    az.add_member_raw(&id, &m.member, &caller.principal).await?;
    az.audit(super::AuditEvent {
        action: "role.member.add".into(),
        resource: id.clone(),
        decision: "allow".into(),
        detail: Some(json!({"member": m.member})),
        ..az.event(&caller)
    });
    Ok(Json(json!({ "role": id, "member": m.member })))
}

async fn remove_member(State(st): St, caller: Caller, Path((id, member)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let az = &st.authz;
    let id = role_id(&id);
    reject_provider_role(&id)?;
    az.require(&caller, Privilege::ManageGrants, &id)?;
    let removed = az.remove_member(&id, &member).await?;
    az.audit(super::AuditEvent {
        action: "role.member.remove".into(),
        resource: id.clone(),
        decision: "allow".into(),
        detail: Some(json!({"member": member})),
        ..az.event(&caller)
    });
    Ok(Json(json!({ "removed": removed })))
}

#[derive(Deserialize)]
struct TokenReq {
    #[serde(default)]
    principal: Option<String>,
    name: String,
    #[serde(default)]
    ttl_secs: Option<i64>,
}

/// Issue an API token. Allowed for yourself, for an agent you may `assume`,
/// or for anyone with `manage_grants` on `server`.
async fn create_token(State(st): St, caller: Caller, Json(t): Json<TokenReq>) -> ApiResult<Json<Value>> {
    let az = &st.authz;
    let target = t.principal.clone().unwrap_or_else(|| caller.principal.clone());
    validate_principal(&target).map_err(|e| ApiError::bad(e.to_string()))?;
    if target.starts_with("role:") {
        return Err(ApiError::bad("tokens are issued to users and agents, not roles"));
    }
    let own = target == caller.principal || Some(&target) == caller.acting_for.as_ref();
    let may_assume = target.starts_with("agent:") && az.decide(&caller, Privilege::Assume, &target).allowed;
    if !own && !may_assume {
        az.require(&caller, Privilege::ManageGrants, "server")?;
    }
    let (id, token) = az.create_token(&target, &t.name, t.ttl_secs, &caller.principal).await?;
    az.audit(super::AuditEvent {
        action: "token.create".into(),
        resource: target.clone(),
        decision: "allow".into(),
        detail: Some(json!({"token_id": id, "name": t.name, "ttl_secs": t.ttl_secs})),
        ..az.event(&caller)
    });
    Ok(Json(json!({ "id": id, "principal": target, "token": token, "note": "store this token now; it is not shown again" })))
}

async fn list_tokens(State(st): St, caller: Caller) -> ApiResult<Json<Value>> {
    let az = &st.authz;
    let all = az.decide(&caller, Privilege::ManageGrants, "server").allowed;
    let list: Vec<Value> = az
        .tokens(None)
        .into_iter()
        .filter(|t| all || t.principal == caller.principal || Some(&t.principal) == caller.acting_for.as_ref())
        .map(|t| json!({"id": t.id, "principal": t.principal, "name": t.name, "expires_at": t.expires_at, "revoked": t.revoked}))
        .collect();
    Ok(Json(json!({ "tokens": list })))
}

async fn revoke_token(State(st): St, caller: Caller, Path(id): Path<i64>) -> ApiResult<Json<Value>> {
    let az = &st.authz;
    let owner = az.tokens(None).into_iter().find(|t| t.id == id).map(|t| t.principal);
    let Some(owner) = owner else { return Err(ApiError::not_found("token not found")) };
    if owner != caller.principal && Some(&owner) != caller.acting_for.as_ref() {
        az.require(&caller, Privilege::ManageGrants, "server")?;
    }
    az.revoke_token(id).await?;
    az.audit(super::AuditEvent {
        action: "token.revoke".into(),
        resource: owner,
        decision: "allow".into(),
        detail: Some(json!({"token_id": id})),
        ..az.event(&caller)
    });
    Ok(Json(json!({ "revoked": id })))
}

#[derive(Deserialize)]
struct AuditQuery {
    limit: Option<i64>,
    principal: Option<String>,
    decision: Option<String>,
    action: Option<String>,
}

async fn audit(State(st): St, caller: Caller, Query(q): Query<AuditQuery>) -> ApiResult<Json<Value>> {
    let az = &st.authz;
    az.require(&caller, Privilege::Select, &super::table_resource("gateway", "system", "audit_log"))?;
    az.flush_audit().await;
    let events: Vec<_> = az
        .audit_events(q.limit.unwrap_or(200).clamp(1, 10_000) * 4)
        .await?
        .into_iter()
        .filter(|e| q.principal.as_ref().is_none_or(|p| &e.principal == p))
        .filter(|e| q.decision.as_ref().is_none_or(|d| &e.decision == d))
        .filter(|e| q.action.as_ref().is_none_or(|a| &e.action == a))
        .take(q.limit.unwrap_or(200).clamp(1, 10_000) as usize)
        .collect();
    Ok(Json(json!({ "events": events })))
}
