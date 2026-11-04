//! End-to-end test: boots the real binary and exercises every surface the way
//! browsers (y-websocket protocol) and agents (HTTP) use it.

use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message as WsMsg;
use yrs::encoding::read::Cursor;
use yrs::sync::protocol::{MessageReader, SyncMessage};
use yrs::sync::Message;
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::Encode;
use yrs::{Doc, GetString, ReadTxn, Text, Transact, Update};

struct Server {
    child: Child,
    base: String,
    #[allow(dead_code)]
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn start(dir: &std::path::Path, port: u16) -> Server {
    let cfg = dir.join("enact-db.toml");
    let blob_dir = dir.join("blob");
    std::fs::write(
        &cfg,
        format!(
            r#"
[server]
listen = "127.0.0.1:{port}"
data_dir = "{data}"
auth_token = "test-token"

[crdt]
compact_after = 5

[blob]
url = "file://{blob}"

[[sources]]
kind = "csv"
name = "customers"
location = "{csv}"
"#,
            data = dir.join("data").display(),
            blob = blob_dir.display(),
            csv = dir.join("customers.csv").display(),
        ),
    )
    .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_enact-db"))
        .arg("--config")
        .arg(&cfg)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn enact-db");
    let base = format!("http://127.0.0.1:{port}");
    for _ in 0..200 {
        if reqwest::get(format!("{base}/health")).await.is_ok() {
            return Server { child, base, port };
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server did not start");
}

fn client() -> reqwest::Client {
    let mut h = reqwest::header::HeaderMap::new();
    h.insert("authorization", "Bearer test-token".parse().unwrap());
    reqwest::Client::builder().default_headers(h).build().unwrap()
}

/// A minimal y-websocket peer built on yrs: keeps a local Doc in sync with the room.
struct Peer {
    doc: Doc,
    ws: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
}

impl Peer {
    async fn connect(port: u16, doc_id: &str, user: &str) -> Peer {
        let url = format!("ws://127.0.0.1:{port}/ws/{doc_id}?user={user}&token=test-token");
        let (ws, _) = tokio_tungstenite::connect_async(url).await.expect("ws connect");
        let mut p = Peer { doc: Doc::new(), ws };
        // y-websocket client opening: send our SyncStep1
        let sv = p.doc.transact().state_vector();
        p.send(Message::Sync(SyncMessage::SyncStep1(sv)).encode_v1()).await;
        p
    }

    async fn send(&mut self, bytes: Vec<u8>) {
        self.ws.send(WsMsg::Binary(bytes.into())).await.unwrap();
    }

    /// Process incoming frames for `ms` milliseconds.
    async fn pump(&mut self, ms: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
        while let Ok(Some(Ok(msg))) = tokio::time::timeout_at(deadline, self.ws.next()).await {
            if let WsMsg::Binary(data) = msg {
                let mut dec = DecoderV1::new(Cursor::new(&data));
                for m in MessageReader::new(&mut dec) {
                    match m.unwrap() {
                        Message::Sync(SyncMessage::SyncStep2(u)) | Message::Sync(SyncMessage::Update(u)) => {
                            self.doc.transact_mut().apply_update(Update::decode_v1(&u).unwrap()).unwrap();
                        }
                        Message::Sync(SyncMessage::SyncStep1(sv)) => {
                            let u = self.doc.transact().encode_state_as_update_v1(&sv);
                            self.send(Message::Sync(SyncMessage::SyncStep2(u)).encode_v1()).await;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    async fn insert(&mut self, index: u32, s: &str) {
        let text = self.doc.get_or_insert_text("content");
        let before = self.doc.transact().state_vector();
        text.insert(&mut self.doc.transact_mut(), index, s);
        let update = self.doc.transact().encode_state_as_update_v1(&before);
        self.send(Message::Sync(SyncMessage::Update(update)).encode_v1()).await;
    }

    fn text(&self) -> String {
        let t = self.doc.get_or_insert_text("content");
        t.get_string(&self.doc.transact())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("customers.csv"), "id,name,owner_agent\n1,Acme,researcher\n2,Globex,writer\n")
        .unwrap();
    let port = free_port();
    let srv = start(dir.path(), port).await;
    let http = client();
    let b = &srv.base;

    // --- auth is enforced
    let r = reqwest::get(format!("{b}/v1/info")).await.unwrap();
    assert_eq!(r.status(), 401);

    // --- AgentFS: files, kv, audit
    let r = http
        .put(format!("{b}/v1/agents/researcher/fs/notes/plan.md"))
        .header("x-agent-id", "researcher")
        .body("# plan\n- step 1\n")
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    let body = http.get(format!("{b}/v1/agents/researcher/fs/notes/plan.md")).send().await.unwrap().text().await.unwrap();
    assert_eq!(body, "# plan\n- step 1\n");
    let dirl: Value = http.get(format!("{b}/v1/agents/researcher/fs/notes")).send().await.unwrap().json().await.unwrap();
    assert_eq!(dirl["entries"][0]["name"], "plan.md");
    http.put(format!("{b}/v1/agents/researcher/kv/goal"))
        .json(&json!({"task": "summarise Q3"}))
        .send()
        .await
        .unwrap();
    let kv: Value = http.get(format!("{b}/v1/agents/researcher/kv/goal")).send().await.unwrap().json().await.unwrap();
    assert_eq!(kv["task"], "summarise Q3");
    let r = http.get(format!("{b}/v1/agents/researcher/fs/../etc/passwd")).send().await.unwrap();
    assert!(r.status().is_client_error());

    // --- SQL federation: join a CSV source with live agent audit data
    let q: Value = http
        .post(format!("{b}/v1/query"))
        .json(&json!({"sql": "SELECT c.name, t.name AS tool FROM customers c JOIN system.agent_tool_calls t \
                               ON t.agent_id = c.owner_agent ORDER BY tool"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let tools: Vec<&str> = q["rows"].as_array().unwrap().iter().map(|r| r["tool"].as_str().unwrap()).collect();
    assert_eq!(tools, vec!["fs.write", "kv.set"], "{q}");
    // read-only by default
    let r = http.post(format!("{b}/v1/query")).json(&json!({"sql": "CREATE TABLE x AS SELECT 1"})).send().await.unwrap();
    assert_eq!(r.status(), 400);

    // --- CRDT: human A + human B over y-websocket, agent over HTTP
    let mut a = Peer::connect(port, "design-doc", "alice").await;
    let mut bob = Peer::connect(port, "design-doc", "bob").await;
    a.pump(200).await;
    bob.pump(200).await;
    a.insert(0, "Hello from Alice. ").await;
    bob.pump(300).await;
    assert_eq!(bob.text(), "Hello from Alice. ");
    // concurrent edits from both humans
    a.insert(0, "[A]").await;
    bob.insert(bob.text().len() as u32, "[B]").await;
    // agent appends through HTTP
    let r = http
        .post(format!("{b}/v1/docs/design-doc/text/content"))
        .header("x-agent-id", "writer")
        .json(&json!({"append": "[agent]"}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    a.pump(400).await;
    bob.pump(400).await;
    assert_eq!(a.text(), bob.text(), "replicas converge");
    let server_text = http.get(format!("{b}/v1/docs/design-doc/text/content")).send().await.unwrap().text().await.unwrap();
    assert_eq!(server_text, a.text());
    for part in ["[A]", "Hello from Alice.", "[B]", "[agent]"] {
        assert!(server_text.contains(part), "missing {part} in {server_text}");
    }
    // agent presence + structured map edit
    http.put(format!("{b}/v1/docs/design-doc/presence"))
        .header("x-agent-id", "writer")
        .json(&json!({"state": {"user": {"name": "writer", "kind": "agent"}}}))
        .send()
        .await
        .unwrap();
    http.patch(format!("{b}/v1/docs/design-doc/map/meta"))
        .header("x-agent-id", "writer")
        .json(&json!({"status": "draft", "reviewers": ["alice", "bob"]}))
        .send()
        .await
        .unwrap();
    let doc: Value = http.get(format!("{b}/v1/docs/design-doc")).send().await.unwrap().json().await.unwrap();
    assert_eq!(doc["roots"]["meta"]["status"], "draft");
    assert!(doc["presence"].as_array().unwrap().iter().any(|p| p["state"]["user"]["name"] == "writer"));

    // --- memory (Turso vectors)
    http.post(format!("{b}/v1/memory"))
        .header("x-agent-id", "researcher")
        .json(&json!([
            {"namespace": "kb", "id": "cats", "content": "cats purr", "embedding": [1.0, 0.0, 0.0]},
            {"namespace": "kb", "id": "dogs", "content": "dogs bark", "embedding": [0.0, 1.0, 0.0]},
            {"namespace": "kb", "id": "fish", "content": "fish swim", "embedding": [0.0, 0.0, 1.0]}
        ]))
        .send()
        .await
        .unwrap();
    let hits: Value = http
        .post(format!("{b}/v1/memory/search"))
        .json(&json!({"namespace": "kb", "embedding": [0.1, 0.9, 0.0], "k": 2}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(hits["hits"][0]["id"], "dogs", "{hits}");
    assert_eq!(hits["hits"][0]["agent_id"], "researcher");

    // --- transcripts
    http.post(format!("{b}/v1/transcripts"))
        .header("x-actor", "alice")
        .json(&json!([
            {"session_id": "s1", "role": "user", "content": "draft the doc"},
            {"session_id": "s1", "role": "assistant", "actor": "agent:writer", "content": "on it"}
        ]))
        .send()
        .await
        .unwrap();
    let t: Value = http.get(format!("{b}/v1/transcripts/s1")).send().await.unwrap().json().await.unwrap();
    assert_eq!(t["entries"].as_array().unwrap().len(), 2);
    // identity comes from the token, not from a client-supplied X-Actor header
    assert_eq!(t["entries"][0]["actor"], "human:admin");

    // --- blob tier: artifacts + sandbox push
    http.put(format!("{b}/v1/blob/reports/q3.txt")).body("Q3 report").send().await.unwrap();
    let got = http.get(format!("{b}/v1/blob/reports/q3.txt")).send().await.unwrap().text().await.unwrap();
    assert_eq!(got, "Q3 report");
    let r = http.post(format!("{b}/v1/agents/researcher/push")).send().await.unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);

    drop(a);
    drop(bob);
    tokio::time::sleep(Duration::from_millis(300)).await; // let the CRDT writer flush
    let final_text = server_text.clone();
    drop(srv);

    // --- restart: CRDT doc + memory + agent state survive
    let srv = start(dir.path(), port).await;
    let b = &srv.base;
    let after = http.get(format!("{b}/v1/docs/design-doc/text/content")).send().await.unwrap().text().await.unwrap();
    assert_eq!(after, final_text, "document persisted across restart");
    let docs: Value = http
        .post(format!("{b}/v1/query"))
        .json(&json!({"sql": "SELECT doc_id, log_entries FROM system.crdt_docs"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(docs["rows"][0]["doc_id"], "design-doc");
    // compact_after = 5: the log must have been folded into snapshots, not grow unbounded
    let entries = docs["rows"][0]["log_entries"].as_i64().unwrap();
    assert!(entries <= 5, "CRDT log not compacted: {entries} entries");

    // --- cloud sandbox flow: wipe local agent, pull from blob, data is back
    std::fs::remove_file(dir.path().join("data/agents/researcher.db")).ok();
    let r = http.post(format!("{b}/v1/agents/researcher/pull")).send().await.unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    let body = http.get(format!("{b}/v1/agents/researcher/fs/notes/plan.md")).send().await.unwrap().text().await.unwrap();
    assert_eq!(body, "# plan\n- step 1\n");
}
