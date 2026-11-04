//! LLM-driven agents against a mock OpenAI-compatible server: the agents get
//! the live context, the model's JSON actions (wrapped in reasoning and code
//! fences, like real models) are executed as CRDT edits, and every exchange is
//! stored in the agent's AgentFS sandbox.

use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

type Seen = Arc<Mutex<Vec<(Option<String>, Value)>>>;

async fn completions(State(seen): State<Seen>, h: HeaderMap, Json(req): Json<Value>) -> Json<Value> {
    let auth = h.get("authorization").and_then(|v| v.to_str().ok()).map(str::to_string);
    seen.lock().unwrap().push((auth, req.clone()));
    let system = req["messages"][0]["content"].as_str().unwrap_or("");
    let content = if system.contains("You are scribe") {
        "<think>They want risks {listed}.</think>\n```json\n{\"reply\": \"Added a Risks section.\", \"actions\": [{\"type\": \"write\", \"text\": \"## Risks\\n- Supplier delay\\n- Budget overrun\"}]}\n```"
            .to_string()
    } else {
        json!({"reply": "Drawing the supply flow.", "actions": [
            {"type": "box", "label": "Supplier"},
            {"type": "ellipse", "label": "Factory"},
            {"type": "arrow", "from": "Supplier", "to": "Factory"},
            {"type": "path", "points": [[100, 660], [200, 630], [300, 660], [400, 630]]},
            {"type": "teleport", "label": "nope"}
        ]})
        .to_string()
    };
    Json(json!({
        "id": "mock", "object": "chat.completion",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
        "base_resp": {"status_code": 0, "status_msg": ""}
    }))
}

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn llm_agents_follow_model_actions() {
    let seen: Seen = Arc::new(Mutex::new(vec![]));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let llm_base = format!("http://{}/v1", listener.local_addr().unwrap());
    let app = Router::new().route("/v1/chat/completions", post(completions)).with_state(seen.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let dir = tempfile::tempdir().unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let tokens = dir.path().join("tokens.json");
    let _srv = Server(
        Command::new(env!("CARGO_BIN_EXE_enact-db"))
            .args(["demo", "--listen", &format!("127.0.0.1:{port}"), "--speed-ms", "2", "--data-dir"])
            .arg(dir.path().join("data"))
            .arg("--tokens-out")
            .arg(&tokens)
            .env("OPENAI_COMPATIBLE_BASE_URL", &llm_base)
            .env("OPENAI_COMPATIBLE_API_KEY", "test-key")
            .env("OPENAI_COMPATIBLE_MODEL", "mock-model")
            .env_remove("OPENAI_COMPATIBLE_MAX_TOKENS")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    for _ in 0..300 {
        if tokens.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let cfg: Value = serde_json::from_slice(&std::fs::read(&tokens).unwrap()).unwrap();
    let base = cfg["base"].as_str().unwrap().to_string();
    let admin = cfg["admin"].as_str().unwrap().to_string();
    let alice = cfg["tokens"]["alice"].as_str().unwrap().to_string();
    let http = reqwest::Client::new();
    let get = |path: String| {
        let (http, admin) = (http.clone(), admin.clone());
        async move { http.get(path).bearer_auth(admin).send().await.unwrap() }
    };

    // wait for the scripted agenda, then alice asks both agents (chat is part of the CRDT doc)
    for _ in 0..200 {
        if get(format!("{base}/v1/docs/demo/text/doc")).await.text().await.unwrap().contains("- Next steps") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for (i, text) in ["@scribe list our main risks", "@sketch draw the supply flow"].iter().enumerate() {
        let r = http
            .post(format!("{base}/v1/docs/demo/array/chat"))
            .bearer_auth(&alice)
            .json(&json!({"push": {"id": format!("a{i}"), "author": "alice", "kind": "human", "color": "#2f6fde", "text": text, "ts": 1}}))
            .send()
            .await
            .unwrap();
        assert!(r.status().is_success());
    }

    // the model's actions show up as live CRDT edits
    let mut text = String::new();
    let mut shapes = Value::Null;
    for _ in 0..300 {
        text = get(format!("{base}/v1/docs/demo/text/doc")).await.text().await.unwrap();
        shapes = get(format!("{base}/v1/docs/demo/array/shapes")).await.json().await.unwrap();
        let has = |kind: &str, label: &str| shapes.as_array().unwrap().iter().any(|s| s["kind"] == kind && s["label"] == label && s["author"] == "sketch");
        let done = text.contains("## Risks\n- Supplier delay\n- Budget overrun")
            && has("rect", "Supplier")
            && has("ellipse", "Factory")
            && shapes.as_array().unwrap().iter().any(|s| s["kind"] == "arrow" && s["author"] == "sketch" && s["x"].as_f64().unwrap() > 200.0)
            && shapes.as_array().unwrap().iter().any(|s| s["kind"] == "pen" && s["points"].as_array().map(|p| p.len()) == Some(8));
        if done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(text.contains("## Risks\n- Supplier delay\n- Budget overrun"), "doc: {text}");
    assert!(shapes.as_array().unwrap().iter().any(|s| s["label"] == "Factory" && s["kind"] == "ellipse"), "{shapes}");
    let chat: Value = get(format!("{base}/v1/docs/demo/array/chat")).await.json().await.unwrap();
    let replies: Vec<String> = chat.as_array().unwrap().iter().map(|m| format!("{}: {}", m["author"], m["text"])).collect();
    assert!(replies.iter().any(|r| r.contains("Added a Risks section.")), "{replies:?}");
    assert!(replies.iter().any(|r| r.contains("Drawing the supply flow.")), "{replies:?}");

    // what the model received
    {
        let seen = seen.lock().unwrap();
        assert!(seen.len() >= 2);
        for (auth, req) in seen.iter() {
            assert_eq!(auth.as_deref(), Some("Bearer test-key"));
            assert_eq!(req["model"], "mock-model");
            assert_eq!(req["max_tokens"], 100000);
            let user = req["messages"][1]["content"].as_str().unwrap();
            assert!(user.contains("Request from alice:"), "{user}");
            assert!(user.contains("## Agenda (scribe)"), "the live document is in the context");
            assert!(user.contains("Participants online:"));
        }
    }

    // every exchange is stored in the agent's own AgentFS sandbox
    let tools: Value = get(format!("{base}/v1/agents/sketch/tools")).await.json().await.unwrap();
    let llm_call = tools["tool_calls"].as_array().unwrap().iter().find(|t| t["name"] == "llm.respond").expect("llm.respond recorded");
    let executed = llm_call["result"]["actions"].as_array().unwrap();
    assert!(executed.iter().any(|a| a.as_str().unwrap().starts_with("skipped: unknown action 'teleport'")), "{llm_call}");
    let listing: Value = get(format!("{base}/v1/agents/scribe/fs/llm")).await.json().await.unwrap();
    let file = listing["entries"][0]["name"].as_str().expect("an /llm/*.json file").to_string();
    let rec: Value = get(format!("{base}/v1/agents/scribe/fs/llm/{file}")).await.json().await.unwrap();
    assert_eq!(rec["model"], "mock-model");
    assert!(rec["raw_response"].as_str().unwrap().contains("<think>"));
    assert_eq!(rec["parsed"]["reply"], "Added a Risks section.");
    assert!(!rec.to_string().contains("test-key"), "the API key is never stored");
}
