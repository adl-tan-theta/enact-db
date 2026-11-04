//! Authentication: who is calling.
//!
//! In order:
//! 1. **Break-glass**: `[auth] admin_token` → `user:admin` (bypasses checks, every action audited).
//! 2. **API tokens** (`edb_…`): issued via `/v1/authz/tokens`, stored as SHA-256 hashes.
//! 3. **OIDC JWTs** (Entra ID, Okta, Keycloak, …): signature (JWKS or HS256 secret), issuer,
//!    audience and expiry are validated. Then **admission gates** run: every
//!    `required_claims` entry must match, or the request is rejected with the
//!    failing claim named. The subject maps to `user:<sub>`, or to `agent:<sub>`
//!    when `agent_claim` matches (e.g. Entra app-only tokens `idtyp = app`). Values
//!    of `roles_claim` become provider-managed roles `role:oidc:<value>`.
//! 4. `allow_anonymous` → `user:anonymous`; otherwise 401.
//!
//! `X-Agent-Id: <id>` then switches the effective principal to `agent:<id>`
//! when the caller *is* that agent or holds `assume` on `agent:<id>`. Every
//! audit event then records both identities.

use super::{Authz, Caller, Denied, Privilege, PROVIDER_ROLE_PREFIX};
use anyhow::Result;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub fn hash_token(t: &str) -> String {
    hex::encode(Sha256::digest(t.as_bytes()))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[derive(Debug, Clone, Deserialize)]
pub struct OidcConfig {
    /// Expected `iss`, e.g. "https://login.microsoftonline.com/<tenant>/v2.0".
    pub issuer: String,
    /// Expected `aud` (one of).
    #[serde(default)]
    pub audience: Vec<String>,
    /// JWKS URL; defaults to the issuer's discovery document.
    #[serde(default)]
    pub jwks_uri: Option<String>,
    /// Env var with a shared HS256 secret (development / internal issuers).
    #[serde(default)]
    pub hs256_secret_env: Option<String>,
    #[serde(default = "default_sub")]
    pub subject_claim: String,
    /// Claim carrying role names (e.g. Entra app roles: "roles"; Keycloak: "groups").
    #[serde(default = "default_roles")]
    pub roles_claim: String,
    /// Treat the token as an agent when `claim == value` (e.g. idtyp = "app").
    #[serde(default)]
    pub agent_claim: Option<ClaimMatch>,
    /// Admission gates: every claim must equal (or, for arrays, contain) the value.
    #[serde(default)]
    pub required_claims: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClaimMatch {
    pub claim: String,
    pub value: String,
}

fn default_sub() -> String {
    "sub".into()
}
fn default_roles() -> String {
    "roles".into()
}

pub struct Oidc {
    cfg: OidcConfig,
    http: reqwest::Client,
    jwks: Mutex<Option<(jsonwebtoken::jwk::JwkSet, Instant)>>,
}

impl Oidc {
    pub fn new(cfg: OidcConfig) -> Result<Self> {
        Ok(Self { cfg, http: reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?, jwks: Mutex::new(None) })
    }

    async fn jwks(&self, force: bool) -> Result<jsonwebtoken::jwk::JwkSet> {
        let mut cache = self.jwks.lock().await;
        if let Some((set, at)) = cache.as_ref() {
            if !force && at.elapsed() < Duration::from_secs(3600) {
                return Ok(set.clone());
            }
        }
        let uri = match &self.cfg.jwks_uri {
            Some(u) => u.clone(),
            None => {
                #[derive(Deserialize)]
                struct Discovery {
                    jwks_uri: String,
                }
                let url = format!("{}/.well-known/openid-configuration", self.cfg.issuer.trim_end_matches('/'));
                let d: Discovery = self.http.get(&url).send().await?.error_for_status()?.json().await?;
                d.jwks_uri
            }
        };
        let set: jsonwebtoken::jwk::JwkSet = self.http.get(&uri).send().await?.error_for_status()?.json().await?;
        *cache = Some((set.clone(), Instant::now()));
        Ok(set)
    }

    /// Validate a JWT and return (principal, provider roles) or an attributable rejection.
    pub async fn verify(&self, token: &str) -> Result<(String, Vec<String>), Denied> {
        let unauthorized = |m: String| Denied { status: 401, message: m };
        let header = decode_header(token).map_err(|e| unauthorized(format!("invalid token: {e}")))?;
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[self.cfg.issuer.as_str()]);
        if self.cfg.audience.is_empty() {
            validation.validate_aud = false;
        } else {
            validation.set_audience(&self.cfg.audience);
        }
        let key = if header.alg == Algorithm::HS256 {
            let var = self.cfg.hs256_secret_env.as_ref().ok_or_else(|| unauthorized("HS256 tokens are not accepted".into()))?;
            let secret = std::env::var(var).map_err(|_| unauthorized(format!("{var} not set")))?;
            DecodingKey::from_secret(secret.as_bytes())
        } else {
            let kid = header.kid.clone().unwrap_or_default();
            let find = |set: &jsonwebtoken::jwk::JwkSet| set.keys.iter().find(|k| k.common.key_id.as_deref() == Some(kid.as_str())).cloned();
            let set = self.jwks(false).await.map_err(|e| unauthorized(format!("jwks: {e}")))?;
            let jwk = match find(&set) {
                Some(k) => k,
                // key rotation: refresh once
                None => find(&self.jwks(true).await.map_err(|e| unauthorized(format!("jwks: {e}")))?)
                    .ok_or_else(|| unauthorized(format!("unknown signing key '{kid}'")))?,
            };
            DecodingKey::from_jwk(&jwk).map_err(|e| unauthorized(format!("jwk: {e}")))?
        };
        let data = decode::<serde_json::Value>(token, &key, &validation).map_err(|e| unauthorized(format!("invalid token: {e}")))?;
        let claims = data.claims;

        // Admission gates (attributable rejections).
        for (claim, want) in &self.cfg.required_claims {
            let ok = match claims.get(claim) {
                Some(serde_json::Value::String(s)) => s.as_str() == want.as_str(),
                Some(serde_json::Value::Array(a)) => a.iter().any(|v| v.as_str() == Some(want.as_str())),
                Some(v) => v.to_string() == *want,
                None => false,
            };
            if !ok {
                return Err(Denied { status: 403, message: format!("admission rejected: claim '{claim}' must be '{want}'") });
            }
        }
        let sub = claims
            .get(&self.cfg.subject_claim)
            .and_then(|v| v.as_str())
            .ok_or_else(|| unauthorized(format!("token has no '{}' claim", self.cfg.subject_claim)))?;
        let is_agent = self
            .cfg
            .agent_claim
            .as_ref()
            .is_some_and(|m| claims.get(&m.claim).and_then(|v| v.as_str()) == Some(m.value.as_str()));
        let principal = format!("{}:{sub}", if is_agent { "agent" } else { "user" });
        let roles = match claims.get(&self.cfg.roles_claim) {
            Some(serde_json::Value::Array(a)) => {
                a.iter().filter_map(|v| v.as_str()).map(|r| format!("{PROVIDER_ROLE_PREFIX}{r}")).collect()
            }
            Some(serde_json::Value::String(s)) => s.split_whitespace().map(|r| format!("{PROVIDER_ROLE_PREFIX}{r}")).collect(),
            _ => vec![],
        };
        super::validate_principal(&principal).map_err(|e| unauthorized(e.to_string()))?;
        Ok((principal, roles))
    }
}

impl Authz {
    /// Resolve a bearer token (or none) to a caller.
    pub async fn authenticate(&self, token: Option<&str>, user_agent: Option<String>, source: &'static str) -> Result<Caller, Denied> {
        let base = |principal: String, roles: Vec<String>, break_glass: bool| Caller {
            principal,
            acting_for: None,
            token_roles: roles,
            break_glass,
            user_agent: user_agent.clone(),
            source,
        };
        let Some(token) = token.filter(|t| !t.is_empty()) else {
            if self.open || self.cfg.allow_anonymous {
                return Ok(base("user:anonymous".into(), vec![], false));
            }
            return Err(Denied { status: 401, message: "authentication required".into() });
        };
        if let Some(admin) = &self.cfg.admin_token {
            if constant_time_eq(token.as_bytes(), admin.as_bytes()) {
                return Ok(base("user:admin".into(), vec![], true));
            }
        }
        if let Some(t) = self.lookup_token(token) {
            return Ok(base(t.principal, vec![], false));
        }
        if token.matches('.').count() == 2 {
            if let Some(oidc) = &self.oidc {
                let (p, roles) = oidc.verify(token).await?;
                return Ok(base(p, roles, false));
            }
        }
        if self.open {
            // dev mode tolerates stray tokens
            return Ok(base("user:anonymous".into(), vec![], false));
        }
        let c = base("unknown".into(), vec![], false);
        self.audit(super::AuditEvent {
            action: "authenticate".into(),
            resource: "server".into(),
            decision: "deny".into(),
            reason: Some("invalid or expired token".into()),
            ..self.event(&c)
        });
        Err(Denied { status: 401, message: "invalid or expired token".into() })
    }

    /// Apply `X-Agent-Id` (act as an agent) and, in open mode, `X-Actor`.
    pub fn assume(&self, mut caller: Caller, agent: Option<&str>, actor: Option<&str>) -> Result<Caller, Denied> {
        if let Some(a) = agent.filter(|a| !a.is_empty()) {
            if !crate::agents::valid_agent_id(a) {
                return Err(Denied { status: 400, message: format!("invalid agent id '{a}'") });
            }
            let target = format!("agent:{a}");
            if caller.principal != target {
                self.require(&caller, Privilege::Assume, &target)?;
                caller.acting_for = Some(caller.principal.clone());
                caller.principal = target;
            }
        } else if let Some(u) = actor.filter(|u| !u.is_empty()) {
            if self.open && caller.principal == "user:anonymous" {
                caller.principal = format!("user:{u}");
            }
        }
        Ok(caller)
    }
}

/// Extract the bearer token from `Authorization: Bearer` or `?token=` (WebSockets).
pub fn bearer(headers: &axum::http::HeaderMap, query: Option<&str>) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
        .map(|s| s.trim().to_string())
        .or_else(|| {
            query.and_then(|q| url::form_urlencoded::parse(q.as_bytes()).find(|(k, _)| k == "token").map(|(_, v)| v.into_owned()))
        })
}
