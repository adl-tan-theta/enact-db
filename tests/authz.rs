//! Permissions end-to-end (Lakekeeper-style model): OIDC admission gates and
//! provider roles, API tokens, roles, grants-as-data with inheritance,
//! ownership-on-create, pass_grants, assume (act as an agent), batch checks,
//! filtered listings, read-only collaborators, Quack, and the audit log.

use futures::SinkExt;
use jsonwebtoken::{encode, EncodingKey, Header};
use serde_json::{json, Value};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message as WsMsg;
use yrs::sync::protocol::SyncMessage;
use yrs::sync::Message;
use yrs::updates::encoder::Encode;
use yrs::{Doc, ReadTxn, Text, Transact};

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

const SECRET: &str = "test-hs256-secret-0123456789abcdef";

fn jwt(sub: &str, tid: &str, roles: &[&str], idtyp: Option<&str>) -> String {
    let exp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() + 600;
    let mut claims = json!({"iss": "https://idp.test", "aud": "enact-db", "sub": sub, "tid": tid, "roles": roles, "exp": exp});
    if let Some(t) = idtyp {
        claims["idtyp"] = json!(t);
    }
    encode(&Header::new(jsonwebtoken::Algorithm::HS256), &claims, &EncodingKey::from_secret(SECRET.as_bytes())).unwrap()
}

struct Api {
    base: String,
    http: reqwest::Client,
}

impl Api {
    async fn call(&self, method: &str, path: &str, token: Option<&str>, agent: Option<&str>, body: Option<Value>) -> (u16, Value) {
        let mut rb = match method {
            "GET" => self.http.get(format!("{}{path}", self.base)),
            "POST" => self.http.post(format!("{}{path}", self.base)),
            "PUT" => self.http.put(format!("{}{path}", self.base)),
            "PATCH" => self.http.patch(format!("{}{path}", self.base)),
            "DELETE" => self.http.delete(format!("{}{path}", self.base)),
            _ => unreachable!(),
        };
        if let Some(t) = token {
            rb = rb.bearer_auth(t);
        }
        if let Some(a) = agent {
            rb = rb.header("x-agent-id", a);
        }
        if let Some(b) = body {
            rb = rb.json(&b);
        }
        let r = rb.send().await.unwrap();
        let status = r.status().as_u16();
        let text = r.text().await.unwrap();
        (status, serde_json::from_str(&text).unwrap_or(Value::String(text)))
    }
    async fn sql(&self, token: &str, q: &str) -> (u16, Value) {
        self.call("POST", "/v1/query", Some(token), None, Some(json!({ "sql": q }))).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn permissions_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("customers.csv"), "id,name\n1,Acme\n2,Globex\n").unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    std::fs::write(
        dir.path().join("cfg.toml"),
        format!(
            r#"
[server]
listen = "127.0.0.1:{port}"
data_dir = "{data}"

[auth]
admin_token = "root-token"

[auth.oidc]
issuer = "https://idp.test"
audience = ["enact-db"]
hs256_secret_env = "IDP_SECRET"
agent_claim = {{ claim = "idtyp", value = "app" }}
required_claims = {{ tid = "contoso" }}

# IdP role "member" may start documents, memories and transcripts
[[auth.grants]]
principal = "role:oidc:member"
resource = "docs"
privilege = "create"

[[auth.roles]]
id = "analysts"
description = "read the gateway's business tables"

[[auth.grants]]
principal = "role:analysts"
resource = "schema:gateway/public"
privilege = "select"

[[sources]]
kind = "csv"
name = "customers"
location = "{csv}"
"#,
            data = dir.path().join("data").display(),
            csv = dir.path().join("customers.csv").display()
        ),
    )
    .unwrap();
    let _srv = Server(
        Command::new(env!("CARGO_BIN_EXE_enact-db"))
            .arg("--config")
            .arg(dir.path().join("cfg.toml"))
            .env("IDP_SECRET", SECRET)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let api = Api { base: format!("http://127.0.0.1:{port}"), http: reqwest::Client::new() };
    for _ in 0..300 {
        if reqwest::get(format!("{}/health", api.base)).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let root = "root-token";

    // --- authentication & admission gates
    assert_eq!(api.call("GET", "/v1/info", None, None, None).await.0, 401);
    assert_eq!(api.call("GET", "/v1/info", Some("edb_nope"), None, None).await.0, 401);
    let (s, b) = api.call("GET", "/v1/authz/whoami", Some(&jwt("alice", "evil-corp", &[], None)), None, None).await;
    assert_eq!(s, 403, "{b}");
    assert!(b["error"].as_str().unwrap().contains("admission rejected: claim 'tid'"), "{b}");
    let alice = jwt("alice", "contoso", &["member"], None);

    // admin: alice joins analysts; tokens for bob and the writer agent
    let (s, b) = api.call("POST", "/v1/authz/roles/analysts/members", Some(root), None, Some(json!({"member": "user:alice"}))).await;
    assert_eq!(s, 200, "{b}");
    let (_, b) = api.call("POST", "/v1/authz/tokens", Some(root), None, Some(json!({"principal": "user:bob", "name": "bob laptop"}))).await;
    let bob = b["token"].as_str().unwrap().to_string();
    let (_, b) = api.call("POST", "/v1/authz/tokens", Some(root), None, Some(json!({"principal": "agent:writer", "name": "writer"}))).await;
    let writer = b["token"].as_str().unwrap().to_string();

    let (_, who) = api.call("GET", "/v1/authz/whoami", Some(&alice), None, None).await;
    assert_eq!(who["principal"], "user:alice");
    assert_eq!(who["roles"], json!(["role:analysts", "role:oidc:member"]), "{who}");

    // --- SQL: per-table select, inherited from a schema grant via a role
    let (s, r) = api.sql(&alice, "SELECT name FROM customers ORDER BY id").await;
    assert_eq!((s, r["rows"].clone()), (200, json!([{"name": "Acme"}, {"name": "Globex"}])));
    let (s, r) = api.sql(&bob, "SELECT * FROM customers").await;
    assert_eq!(s, 403, "{r}");
    assert!(r["error"].as_str().unwrap().contains("user:bob lacks 'select' on 'table:gateway/public/customers'"));
    // a subquery cannot smuggle in a table the caller can't read
    let (s, _) = api.sql(&alice, "SELECT name FROM customers WHERE id IN (SELECT id FROM system.agent_tool_calls)").await;
    assert_eq!(s, 403);
    let (s, _) = api.sql(&alice, "SELECT * FROM information_schema.tables").await;
    assert_eq!(s, 403, "information_schema reveals table names: needs describe on the catalog");

    // --- documents: ownership on create, explicit sharing, read-only collaborators
    let (s, b) = api.call("POST", "/v1/docs/plan/text/content", Some(&alice), None, Some(json!({"append": "Q4 plan."}))).await;
    assert_eq!(s, 200, "alice (role:oidc:member has create on docs) starts the doc: {b}");
    let (_, g) = api.call("GET", "/v1/authz/grants?resource=doc:plan", Some(&alice), None, None).await;
    assert_eq!(g["grants"][0]["principal"], "user:alice");
    assert_eq!(g["grants"][0]["privilege"], "ownership");
    assert_eq!(api.call("GET", "/v1/docs/plan/text/content", Some(&bob), None, None).await.0, 403);
    assert_eq!(api.call("POST", "/v1/docs/other/text/content", Some(&bob), None, Some(json!({"append": "x"}))).await.0, 403, "bob may not create docs");

    let (s, _) = api.call("POST", "/v1/authz/grants", Some(&alice), None, Some(json!({"principal": "user:bob", "resource": "doc:plan", "privilege": "select"}))).await;
    assert_eq!(s, 200, "the owner can share");
    let (s, t) = api.call("GET", "/v1/docs/plan/text/content", Some(&bob), None, None).await;
    assert_eq!((s, t), (200, json!("Q4 plan.")));
    assert_eq!(api.call("POST", "/v1/docs/plan/text/content", Some(&bob), None, Some(json!({"append": "!"}))).await.0, 403);

    // bob over WebSocket is a read-only peer: his updates are dropped
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws/plan?token={bob}")).await.unwrap();
    let doc = Doc::new();
    let txt = doc.get_or_insert_text("content");
    txt.push(&mut doc.transact_mut(), "HACKED ");
    let update = doc.transact().encode_state_as_update_v1(&yrs::StateVector::default());
    ws.send(WsMsg::Binary(Message::Sync(SyncMessage::Update(update)).encode_v1().into())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (_, t) = api.call("GET", "/v1/docs/plan/text/content", Some(&alice), None, None).await;
    assert_eq!(t, json!("Q4 plan."), "read-only peer must not change the document");
    let s = match tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws/plan?token={writer}")).await {
        Ok(_) => 101,
        Err(tokio_tungstenite::tungstenite::Error::Http(r)) => r.status().as_u16(),
        Err(_) => 0,
    };
    assert_eq!(s, 403, "no access, no socket");

    // --- pass_grants: re-share what you hold, nothing more
    let carol_select = json!({"principal": "user:carol", "resource": "doc:plan", "privilege": "select"});
    assert_eq!(api.call("POST", "/v1/authz/grants", Some(&bob), None, Some(carol_select.clone())).await.0, 403);
    api.call("POST", "/v1/authz/grants", Some(&alice), None, Some(json!({"principal": "user:bob", "resource": "doc:plan", "privilege": "pass_grants"}))).await;
    assert_eq!(api.call("POST", "/v1/authz/grants", Some(&bob), None, Some(carol_select)).await.0, 200);
    assert_eq!(
        api.call("POST", "/v1/authz/grants", Some(&bob), None, Some(json!({"principal": "user:carol", "resource": "doc:plan", "privilege": "write_data"}))).await.0,
        403,
        "bob cannot pass on write_data he does not hold"
    );

    // --- agents: self-owned sandbox, isolation, and `assume`
    assert_eq!(api.call("PUT", "/v1/agents/writer/kv/style", Some(&writer), None, Some(json!("concise"))).await.0, 200);
    assert_eq!(api.call("GET", "/v1/agents/researcher/kv", Some(&writer), None, None).await.0, 403);
    assert_eq!(api.call("POST", "/v1/docs/plan/text/content", Some(&alice), Some("writer"), Some(json!({"append": "x"}))).await.0, 403, "alice may not act as writer yet");
    api.call("POST", "/v1/authz/grants", Some(root), None, Some(json!({"principal": "user:alice", "resource": "agent:writer", "privilege": "assume"}))).await;
    api.call("POST", "/v1/authz/grants", Some(&alice), None, Some(json!({"principal": "agent:writer", "resource": "doc:plan", "privilege": "write_data"}))).await;
    let (s, b) = api.call("POST", "/v1/docs/plan/text/content", Some(&alice), Some("writer"), Some(json!({"append": " Drafted by writer."}))).await;
    assert_eq!(s, 200, "{b}");
    let (_, t) = api.call("GET", "/v1/docs/plan/text/content", Some(&alice), None, None).await;
    assert_eq!(t, json!("Q4 plan. Drafted by writer."));

    // --- batch check with determined_by
    let (_, c) = api
        .call(
            "POST",
            "/v1/authz/check",
            Some(&alice),
            None,
            Some(json!({"checks": [
                {"resource": "doc:plan", "privilege": "write_data"},
                {"resource": "table:gateway/public/customers", "privilege": "select"},
                {"resource": "table:gateway/system/audit_log", "privilege": "select"}
            ]})),
        )
        .await;
    let res = c["results"].as_array().unwrap();
    assert_eq!(res.iter().map(|r| r["allowed"].as_bool().unwrap()).collect::<Vec<_>>(), vec![true, true, false]);
    assert!(res[0]["determined_by"].as_str().unwrap().contains("user:alice ownership on doc:plan"), "{c}");
    assert!(res[1]["determined_by"].as_str().unwrap().contains("role:analysts select on schema:gateway/public"), "{c}");
    // checking someone else's access needs manage_grants on the resource
    let (s, _) = api.call("POST", "/v1/authz/check", Some(&bob), None, Some(json!({"checks": [{"resource": "doc:plan", "privilege": "select", "principal": "user:alice"}]}))).await;
    assert_eq!(s, 403);

    // --- listings only show what you may see
    let (_, info) = api.call("GET", "/v1/info", Some(&bob), None, None).await;
    assert_eq!(info["tables"], json!([]), "{info}");
    let (_, info) = api.call("GET", "/v1/info", Some(&alice), None, None).await;
    assert_eq!(info["tables"], json!([{"catalog": "gateway", "schema": "public", "table": "customers"}]));
    let (_, docs) = api.call("GET", "/v1/docs", Some(&bob), None, None).await;
    assert_eq!(docs["persisted"].as_array().unwrap().len(), 1, "bob sees doc:plan (shared with him)");

    // --- tokens: self-service, scoped, revocable
    let (s, b) = api.call("POST", "/v1/authz/tokens", Some(&alice), None, Some(json!({"name": "alice cli"}))).await;
    assert_eq!(s, 200);
    let alice_api = b["token"].as_str().unwrap().to_string();
    let alice_token_id = b["id"].as_i64().unwrap();
    assert_eq!(api.sql(&alice_api, "SELECT count(*) AS n FROM customers").await.0, 200);
    assert_eq!(api.call("POST", "/v1/authz/tokens", Some(&bob), None, Some(json!({"principal": "user:alice", "name": "steal"}))).await.0, 403);
    assert_eq!(api.call("DELETE", &format!("/v1/authz/tokens/{alice_token_id}"), Some(&alice), None, None).await.0, 200);
    assert_eq!(api.sql(&alice_api, "SELECT 1").await.0, 401, "revoked tokens stop working immediately");

    // --- provider-managed roles cannot be edited locally
    assert_eq!(api.call("POST", "/v1/authz/roles/oidc:member/members", Some(root), None, Some(json!({"member": "user:bob"}))).await.0, 403);

    // --- Quack: the catalog is filtered per principal, queries are checked
    let quack = |token: String| {
        let url = format!("{}/quack", api.base);
        async move {
            let http = reqwest::Client::new();
            let mut c = vec![1, 0, 1, 3, 0];
            c.extend([0xFF; 9]);
            c.extend([0x01, 0xFF, 0xFF, 1, 0, token.len() as u8]);
            c.extend(token.as_bytes());
            c.extend([4, 0, 3, 5, 0, 3, 7, 0, 60, 0xFF, 0xFF]);
            let resp = http.post(&url).body(c).send().await.unwrap().bytes().await.unwrap().to_vec();
            assert_eq!(&resp[..3], &[1, 0, 2], "connected: {}", String::from_utf8_lossy(&resp));
            let conn = String::from_utf8(resp[6..6 + resp[5] as usize].to_vec()).unwrap();
            let prepare = |sql: &str| {
                let mut m = vec![1, 0, 3, 2, 0, conn.len() as u8];
                m.extend(conn.as_bytes());
                m.extend([3, 0]);
                m.extend([0xFF; 9]);
                m.extend([0x01, 0xFF, 0xFF, 1, 0, sql.len() as u8]);
                m.extend(sql.as_bytes());
                m.extend([2, 0, 0, 1, 0xFF, 0xFF]);
                m
            };
            let tables = http.post(&url).body(prepare("SELECT schema_oid, sql, 'table' FROM duckdb_tables()")).send().await.unwrap().bytes().await.unwrap();
            let scan = http.post(&url).body(prepare("SELECT #2 FROM main.customers")).send().await.unwrap().bytes().await.unwrap();
            (String::from_utf8_lossy(&tables).to_string(), scan.to_vec())
        }
    };
    let (_, b) = api.call("POST", "/v1/authz/tokens", Some(&alice), None, Some(json!({"name": "duckdb"}))).await;
    let (tables, scan) = quack(b["token"].as_str().unwrap().to_string()).await;
    assert!(tables.contains("CREATE TABLE \"customers\""));
    assert!(!tables.contains("agent_tool_calls"), "alice cannot see system tables");
    assert_eq!(&scan[..3], &[1, 0, 4], "alice scans customers");
    let (tables, scan) = quack(bob.clone()).await;
    assert!(!tables.contains("customers"));
    assert_eq!(&scan[..3], &[1, 0, 100], "bob's scan is a permission error");
    assert!(String::from_utf8_lossy(&scan).contains("Permission"));

    // --- audit log: who, acting for whom, what, decided by which grant
    let (s, a) = api.call("GET", "/v1/authz/audit?decision=deny&principal=user:bob&limit=50", Some(root), None, None).await;
    assert_eq!(s, 200);
    let ev = a["events"].as_array().unwrap();
    assert!(ev.iter().any(|e| e["action"] == "select" && e["resource"] == "table:gateway/public/customers"), "{a}");
    assert!(ev.iter().all(|e| e["version"] == 1));
    let (_, a) = api.call("GET", "/v1/authz/audit?principal=agent:writer&limit=200", Some(root), None, None).await;
    assert!(
        a["events"].as_array().unwrap().iter().any(|e| e["acting_for"] == "user:alice" && e["action"] == "write_data" && e["decision"] == "allow"),
        "assumed actions record both identities: {a}"
    );
    assert_eq!(api.call("GET", "/v1/authz/audit", Some(&alice), None, None).await.0, 403);
    let (s, r) = api.sql(root, "SELECT count(*) AS n FROM system.audit_log WHERE decision = 'deny'").await;
    assert_eq!(s, 200);
    assert!(r["rows"][0]["n"].as_i64().unwrap() >= 8, "{r}");
    let (_, r) = api.sql(root, "SELECT principal, privilege FROM system.grants WHERE resource = 'doc:plan' ORDER BY id").await;
    assert_eq!(
        r["rows"],
        json!([
            {"principal": "user:alice", "privilege": "ownership"},
            {"principal": "user:bob", "privilege": "select"},
            {"principal": "user:bob", "privilege": "pass_grants"},
            {"principal": "user:carol", "privilege": "select"},
            {"principal": "agent:writer", "privilege": "write_data"}
        ])
    );
    let _ = ws.close(None).await;
}
