//! OpenAI-compatible chat-completions client for agent "brains".
//!
//! Works with any provider that speaks `POST {base}/chat/completions`
//! (MiniMax, OpenAI, Azure OpenAI-compatible gateways, vLLM, Ollama, LiteLLM, ...).
//! Configuration:
//! * `OPENAI_COMPATIBLE_BASE_URL`, e.g. `https://api.minimax.io/v1`
//! * `OPENAI_COMPATIBLE_API_KEY`
//! * `OPENAI_COMPATIBLE_MODEL` or `--model` (default: `MiniMax-M3`)
//! * `OPENAI_COMPATIBLE_MAX_TOKENS` (default 100000: room for long reasoning)
//!
//! Models answer with a JSON object of actions. Replies are parsed
//! leniently: reasoning blocks (`<think>…</think>`), markdown code fences and
//! surrounding prose are stripped before the first JSON object is decoded.

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

pub const DEFAULT_MODEL: &str = "MiniMax-M3";
/// Embedding model for agent memory (OpenRouter). Alternatives:
/// `nvidia/nemotron-3.5-embed-8b:free`, `amazon/titan-embed-text-v1`.
pub const DEFAULT_EMBED_MODEL: &str = "nvidia/nemotron-3.5-embed-1b:free";
pub const DEFAULT_MAX_TOKENS: u32 = 100_000;

#[derive(Clone)]
pub struct Llm {
    http: reqwest::Client,
    base: String,
    key: String,
    pub model: String,
    pub max_tokens: u32,
}

impl std::fmt::Debug for Llm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // never print the key
        f.debug_struct("Llm").field("base", &self.base).field("model", &self.model).field("max_tokens", &self.max_tokens).finish()
    }
}

pub struct Completion {
    pub text: String,
    pub usage: Value,
    pub latency_ms: u128,
}

impl Llm {
    /// Build from the environment; `None` if the base URL or key is missing.
    pub fn from_env(model: Option<String>) -> Option<Self> {
        let base = std::env::var("OPENAI_COMPATIBLE_BASE_URL").ok().filter(|s| !s.trim().is_empty())?;
        let key = std::env::var("OPENAI_COMPATIBLE_API_KEY").ok().filter(|s| !s.trim().is_empty())?;
        let model = model
            .or_else(|| std::env::var("OPENAI_COMPATIBLE_MODEL").ok())
            .filter(|m| !m.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let max_tokens = std::env::var("OPENAI_COMPATIBLE_MAX_TOKENS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .filter(|n: &u32| *n > 0)
            .unwrap_or(DEFAULT_MAX_TOKENS);
        // reasoning models can think for a while on large budgets
        let http = reqwest::Client::builder().timeout(Duration::from_secs(600)).build().ok()?;
        Some(Self { http, base: base.trim_end_matches('/').to_string(), key, model, max_tokens })
    }

    /// Chat with a full message list (system, history turns, current request).
    pub async fn chat(&self, messages: Vec<Value>) -> Result<Completion> {
        let started = Instant::now();
        let body = json!({
            "model": self.model,
            "messages": messages,
            "temperature": 0.4,
            "max_tokens": self.max_tokens,
        });
        let resp = self
            .http
            .post(format!("{}/chat/completions", self.base))
            .bearer_auth(&self.key)
            .json(&body)
            .send()
            .await
            .context("LLM request failed")?;
        let status = resp.status();
        let v: Value = resp.json().await.context("LLM response was not JSON")?;
        if !status.is_success() {
            let msg = v.pointer("/error/message").or_else(|| v.get("base_resp")).cloned().unwrap_or(v.clone());
            bail!("LLM HTTP {status}: {msg}");
        }
        // MiniMax reports some errors with HTTP 200 + base_resp.status_code != 0
        if let Some(code) = v.pointer("/base_resp/status_code").and_then(|c| c.as_i64()) {
            if code != 0 {
                bail!("LLM error {code}: {}", v.pointer("/base_resp/status_msg").and_then(|m| m.as_str()).unwrap_or(""));
            }
        }
        let text = v
            .pointer("/choices/0/message/content")
            .and_then(|c| c.as_str())
            .ok_or_else(|| anyhow!("LLM response has no choices[0].message.content"))?
            .to_string();
        Ok(Completion { text, usage: v.get("usage").cloned().unwrap_or(Value::Null), latency_ms: started.elapsed().as_millis() })
    }
}

/// OpenAI-compatible embeddings client (OpenRouter by default) for agent memory.
/// * `OPENROUTER_API_KEY` (required), `OPENROUTER_BASE_URL` (default `https://openrouter.ai/api/v1`)
/// * `OPENROUTER_EMBED_MODEL` or `--embed-model` (default [`DEFAULT_EMBED_MODEL`])
#[derive(Clone)]
pub struct Embedder {
    http: reqwest::Client,
    base: String,
    key: String,
    pub model: String,
}

impl std::fmt::Debug for Embedder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Embedder").field("base", &self.base).field("model", &self.model).finish()
    }
}

impl Embedder {
    pub fn from_env(model: Option<String>) -> Option<Self> {
        let key = std::env::var("OPENROUTER_API_KEY").ok().filter(|s| !s.trim().is_empty())?;
        let base = std::env::var("OPENROUTER_BASE_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "https://openrouter.ai/api/v1".into());
        let model = model
            .or_else(|| std::env::var("OPENROUTER_EMBED_MODEL").ok())
            .filter(|m| !m.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_EMBED_MODEL.to_string());
        let http = reqwest::Client::builder().timeout(Duration::from_secs(60)).build().ok()?;
        Some(Self { http, base: base.trim_end_matches('/').to_string(), key, model })
    }

    pub async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let resp = self
            .http
            .post(format!("{}/embeddings", self.base))
            .bearer_auth(&self.key)
            .json(&json!({"model": self.model, "input": texts}))
            .send()
            .await
            .context("embedding request failed")?;
        let status = resp.status();
        let v: Value = resp.json().await.context("embedding response was not JSON")?;
        if !status.is_success() || v.get("error").is_some() {
            bail!("embeddings HTTP {status}: {}", v.pointer("/error/message").cloned().unwrap_or(v.clone()));
        }
        let mut rows: Vec<(usize, Vec<f32>)> = v["data"]
            .as_array()
            .ok_or_else(|| anyhow!("embedding response has no data"))?
            .iter()
            .enumerate()
            .map(|(i, d)| {
                let idx = d["index"].as_u64().map(|x| x as usize).unwrap_or(i);
                let vec = d["embedding"].as_array().map(|a| a.iter().filter_map(|x| x.as_f64()).map(|x| x as f32).collect()).unwrap_or_default();
                (idx, vec)
            })
            .collect();
        rows.sort_by_key(|r| r.0);
        if rows.len() != texts.len() || rows.iter().any(|r| r.1.is_empty()) {
            bail!("embedding response has {} vectors for {} inputs", rows.len(), texts.len());
        }
        Ok(rows.into_iter().map(|r| r.1).collect())
    }

    pub async fn embed_one(&self, text: &str) -> Result<Vec<f32>> {
        Ok(self.embed(&[text.to_string()]).await?.remove(0))
    }

    /// Vector size of the model (needed to create the LanceDB table).
    pub async fn dimensions(&self) -> Result<usize> {
        Ok(self.embed_one("dimension probe").await?.len())
    }
}

/// Strip reasoning blocks and fences, then decode the first JSON object.
pub fn extract_json(raw: &str) -> Option<Value> {
    let mut s = raw.to_string();
    // remove <think>...</think> (possibly several, possibly unterminated)
    while let Some(start) = s.find("<think>") {
        match s[start..].find("</think>") {
            Some(end) => s.replace_range(start..start + end + "</think>".len(), ""),
            None => s.truncate(start),
        }
    }
    let s = s.trim();
    if let Ok(v) = serde_json::from_str::<Value>(s) {
        if v.is_object() {
            return Some(v);
        }
    }
    // first balanced {...} (string-aware)
    let bytes = s.as_bytes();
    let mut start = None;
    let (mut depth, mut in_str, mut esc) = (0i32, false, false);
    for (i, &b) in bytes.iter().enumerate() {
        if in_str {
            match (esc, b) {
                (true, _) => esc = false,
                (false, b'\\') => esc = true,
                (false, b'"') => in_str = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_str = true,
            b'{' => {
                if depth == 0 {
                    start = Some(i);
                }
                depth += 1;
            }
            b'}' if depth > 0 => {
                depth -= 1;
                if depth == 0 {
                    if let Some(st) = start {
                        if let Ok(v) = serde_json::from_str::<Value>(&s[st..=i]) {
                            return Some(v);
                        }
                        start = None;
                    }
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_reasoning_and_fences() {
        let raw = "<think>user wants {a box}</think>\nSure!\n```json\n{\"reply\": \"ok {fine}\", \"actions\": [{\"type\": \"box\", \"label\": \"A\"}]}\n```";
        let v = extract_json(raw).unwrap();
        assert_eq!(v["reply"], "ok {fine}");
        assert_eq!(v["actions"][0]["label"], "A");
        assert!(extract_json("<think>never closed {").is_none());
        assert_eq!(extract_json("{\"reply\":\"x\"}").unwrap()["reply"], "x");
    }
}
