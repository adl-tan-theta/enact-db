//! Agent memory across turns and restarts: session history in AgentFS (Turso)
//! and long-term semantic memory in LanceDB, with a mock OpenAI-compatible
//! chat model and a mock OpenRouter-compatible embeddings endpoint.

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

type Seen = Arc<Mutex<Vec<Value>>>;
const DIMS: usize = 16;

/// Deterministic bag-of-words embedding: shared words => similar vectors.
fn embed(text: &str) -> Vec<f32> {
    let mut v = vec![0f32; DIMS];
    for w in text.to_lowercase().split(|c: char| !c.is_alphanumeric()).filter(|w| w.len() > 2) {
        let h = w.bytes().fold(2166136261u32, |h, b| (h ^ b as u32).wrapping_mul(16777619));
        v[(h as usize) % DIMS] += 1.0;
    }
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
    v.iter().map(|x| x / n).collect()
}

async fn embeddings(Json(req): Json<Value>) -> Json<Value> {
    assert!(req["model"].as_str().unwrap().contains("nemotron"), "default embedding model");
    // like OpenRouter's free endpoints: a single string works, batches do not
    let data: Vec<Value> = match &req["input"] {
        Value::String(t) => vec![json!({"index": 0, "embedding": embed(t)})],
        _ => return Json(json!({"error": {"message": "batched input not supported", "code": 502}})),
    };
    Json(json!({"data": data, "model": req["model"]}))
}

async fn chat(State(seen): State<Seen>, Json(req): Json<Value>) -> Json<Value> {
    seen.lock().unwrap().push(req.clone());
    let msgs = req["messages"].as_array().unwrap();
    let full = msgs.last().unwrap()["content"].as_str().unwrap_or("");
    // the current request line (the prompt also carries recent chat and memories)
    let request = full.lines().find(|l| l.starts_with("Request from ")).unwrap_or("");
    let last = request;
    let system = msgs[0]["content"].as_str().unwrap_or("");
    let out = if !system.contains("You are scribe") {
        json!({"reply": "", "actions": []})
    } else if last.contains("Request from alice: @scribe write the Atlas launch plan") {
        json!({"reply": "Wrote the Atlas plan.", "actions": [{"type": "write", "text": "## Atlas launch\n- Build in July\n- Beta in August\n- Launch in September with a press event"}]})
    } else if last.contains("make it shorter") {
        // a follow-up only makes sense with the previous turn in the history
        let history_has_atlas = msgs.iter().any(|m| m["role"] == "assistant" && m["content"].as_str().unwrap_or("").contains("Atlas"));
        json!({"reply": if history_has_atlas { "Shortened the Atlas plan." } else { "Shorten what?" },
               "actions": [{"type": "write", "text": "## Atlas (short)\n- Jul build, Aug beta, Sep launch"}]})
    } else if last.contains("remember that") {
        json!({"reply": "Noted.", "actions": [{"type": "remember", "text": "Decision: the Atlas launch budget is 40k EUR, approved by bob"}]})
    } else if last.contains("what is the Atlas budget") {
        let recalled = full.contains("Atlas launch budget is 40k EUR");
        json!({"reply": if recalled { "The Atlas budget is 40k EUR (approved by bob)." } else { "I don't know." }, "actions": []})
    } else {
        json!({"reply": "ok", "actions": []})
    };
    Json(json!({"choices": [{"message": {"role": "assistant", "content": format!("<think>...</think>{out}")}}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5}}))
}

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(data: &std::path::Path, tokens: &std::path::Path, mock: &str) -> Server {
    let _ = std::fs::remove_file(tokens);
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    Server(
        Command::new(env!("CARGO_BIN_EXE_enact-db"))
            .args(["demo", "--listen", &format!("127.0.0.1:{port}"), "--speed-ms", "1", "--data-dir"])
            .arg(data)
            .arg("--tokens-out")
            .arg(tokens)
            .env("OPENAI_COMPATIBLE_BASE_URL", format!("{mock}/v1"))
            .env("OPENAI_COMPATIBLE_API_KEY", "llm-key")
            .env("OPENAI_COMPATIBLE_MODEL", "mock-model")
            .env("OPENROUTER_BASE_URL", format!("{mock}/v1"))
            .env("OPENROUTER_API_KEY", "embed-key")
            .env_remove("OPENROUTER_EMBED_MODEL")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    )
}

async fn wait_tokens(tokens: &std::path::Path) -> Value {
    for _ in 0..600 {
        if let Ok(b) = std::fs::read(tokens) {
            if let Ok(v) = serde_json::from_slice::<Value>(&b) {
                return v;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("demo did not start");
}

async fn ask(http: &reqwest::Client, cfg: &Value, text: &str) {
    let r = http
        .post(format!("{}/v1/docs/demo/array/chat", cfg["base"].as_str().unwrap()))
        .bearer_auth(cfg["tokens"]["alice"].as_str().unwrap())
        .json(&json!({"push": {"id": text, "author": "alice", "kind": "human", "color": "#2f6fde", "text": text, "ts": 1}}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
}

async fn chat_log(http: &reqwest::Client, cfg: &Value) -> Vec<String> {
    let v: Value = http
        .get(format!("{}/v1/docs/demo/array/chat", cfg["base"].as_str().unwrap()))
        .bearer_auth(cfg["admin"].as_str().unwrap())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    v.as_array().unwrap().iter().map(|m| format!("{}: {}", m["author"].as_str().unwrap(), m["text"].as_str().unwrap())).collect()
}

async fn wait_reply(http: &reqwest::Client, cfg: &Value, needle: &str) {
    for _ in 0..300 {
        if chat_log(http, cfg).await.iter().any(|m| m.starts_with("scribe: ") && m.contains(needle)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no scribe reply containing {needle:?}; chat: {:#?}", chat_log(http, cfg).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn agent_memory_across_turns_and_restarts() {
    let seen: Seen = Arc::new(Mutex::new(vec![]));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/v1/chat/completions", post(chat))
        .route("/v1/embeddings", post(embeddings))
        .with_state(seen.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let dir = tempfile::tempdir().unwrap();
    let (data, tokens) = (dir.path().join("data"), dir.path().join("tokens.json"));
    let http = reqwest::Client::new();

    // --- session 1
    {
        let _srv = start(&data, &tokens, &mock);
        let cfg = wait_tokens(&tokens).await;
        tokio::time::sleep(Duration::from_millis(1500)).await; // agents join
        ask(&http, &cfg, "@scribe write the Atlas launch plan").await;
        wait_reply(&http, &cfg, "Wrote the Atlas plan.").await;
        ask(&http, &cfg, "@scribe make it shorter").await;
        wait_reply(&http, &cfg, "Shortened the Atlas plan.").await; // only if the history carried turn 1
        ask(&http, &cfg, "@scribe remember that bob approved a 40k EUR budget for Atlas").await;
        wait_reply(&http, &cfg, "Noted.").await;

        // session transcript lives in scribe's AgentFS (Turso)
        let base = cfg["base"].as_str().unwrap();
        // (the agent replies first, then executes and records the turn)
        let mut t = String::new();
        for _ in 0..100 {
            t = http
                .get(format!("{base}/v1/agents/scribe/fs/sessions/demo.jsonl"))
                .bearer_auth(cfg["admin"].as_str().unwrap())
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
            if t.lines().count() >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await; // turn memory is embedded after the transcript append
        let turns: Vec<Value> = t.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        assert_eq!(turns.len(), 3, "{t}");
        assert!(turns[2]["executed"][0].as_str().unwrap().starts_with("remembered \"Decision: the Atlas launch budget is 40k EUR"), "{t}");
        // long-term memories are in LanceDB (namespace agent-scribe)
        let q: Value = http
            .post(format!("{base}/v1/memory/search"))
            .bearer_auth(cfg["tokens"]["scribe"].as_str().unwrap())
            .json(&json!({"namespace": "agent-scribe", "embedding": embed("Atlas budget"), "k": 10}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let hits = q["hits"].as_array().unwrap();
        assert!(hits.len() >= 3, "turns + fact stored: {q}");
        assert!(hits.iter().any(|h| h["metadata"]["kind"] == "fact" && h["content"].as_str().unwrap().contains("40k EUR")));
        // alice may not read the agent's private memory
        let r = http
            .post(format!("{base}/v1/memory/search"))
            .bearer_auth(cfg["tokens"]["alice"].as_str().unwrap())
            .json(&json!({"namespace": "agent-scribe", "embedding": embed("Atlas"), "k": 3}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 403);
    }
    assert!(data.join("lancedb").read_dir().unwrap().next().is_some(), "LanceDB dataset on disk");

    // --- session 2: restart, the agent still remembers (LanceDB + AgentFS)
    let _srv = start(&data, &tokens, &mock);
    let cfg = wait_tokens(&tokens).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    ask(&http, &cfg, "@scribe what is the Atlas budget?").await;
    wait_reply(&http, &cfg, "The Atlas budget is 40k EUR").await;

    let seen = seen.lock().unwrap();
    let req = seen.iter().rev().find(|r| r["messages"].as_array().unwrap().last().unwrap()["content"].as_str().unwrap().contains("what is the Atlas budget")).unwrap();
    let msgs = req["messages"].as_array().unwrap();
    let last = msgs.last().unwrap()["content"].as_str().unwrap();
    assert!(last.starts_with("Relevant long-term memories"), "{last}");
    assert!(msgs.len() >= 8, "system + 3 earlier turns (user/assistant) + current: {}", msgs.len());
    assert_eq!(req["model"], "mock-model");
}
