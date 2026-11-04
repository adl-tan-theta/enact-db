//! A demo agent that joins a shared document as a real CRDT peer.
//!
//! It speaks the same y-websocket protocol as the browser (sync step 1/2,
//! updates, awareness), authenticates with its own token (principal
//! `agent:<name>`), and is subject to the same permissions as anyone else.
//! It shows presence, a text cursor that follows concurrent edits (sticky
//! index), and a pointer while it draws. Everything it does is recorded in
//! its AgentFS tool-call log and in the session transcript.
//!
//! Shared document schema (also used by `/demo`):
//! * `doc`    – Y.Text, the shared document
//! * `shapes` – Y.Array of Y.Map {id, kind: pen|rect|ellipse|arrow|note, color,
//!   author, x, y, w, h, x2, y2, label, points: Y.Array<number>}
//! * `chat`   – Y.Array of {id, author, kind: human|agent, color, text, ts}
//!
//! Personas:
//! * `scribe` writes: opens an agenda, then answers `@scribe summarize` and
//!   `@scribe write <text>`.
//! * `sketch` draws: a box for every section heading in the document (linked
//!   by arrows), plus `@sketch box|circle|note <label>`, `@sketch smile`,
//!   `@sketch diagram` and `@sketch clear`.

use anyhow::{anyhow, bail, Context, Result};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify};
use tokio_tungstenite::tungstenite::Message as WsMsg;
use yrs::encoding::read::Cursor;
use yrs::sync::protocol::{MessageReader, SyncMessage};
use yrs::sync::{Awareness, Message};
use yrs::types::ToJson;
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::Encode;
use yrs::{
    Any, Array, ArrayPrelim, ArrayRef, Assoc, Doc, GetString, IndexedSequence, Map, MapPrelim, MapRef, OffsetKind,
    Options, Out, ReadTxn, Text, TextRef, Transact, Update,
};

#[derive(clap::Args, Debug, Clone)]
pub struct AgentArgs {
    /// enact-db base URL
    #[arg(long, default_value = "http://127.0.0.1:9494")]
    pub server: String,
    /// The agent's API token (principal agent:<name>)
    #[arg(long, env = "ENACT_DB_AGENT_TOKEN")]
    pub token: String,
    /// Agent id (must match the token's principal)
    #[arg(long)]
    pub name: String,
    /// Behaviour: scribe (writes) or sketch (draws)
    #[arg(long, default_value = "scribe")]
    pub persona: String,
    /// Shared document id
    #[arg(long, default_value = "demo")]
    pub doc: String,
    /// Milliseconds per typed character / animation step
    #[arg(long, default_value_t = 35)]
    pub speed_ms: u64,
    /// Presence colour
    #[arg(long)]
    pub color: Option<String>,
    /// LLM model for the agent's brain (OpenAI-compatible; see `demo::llm`)
    #[arg(long, env = "OPENAI_COMPATIBLE_MODEL")]
    pub model: Option<String>,
    /// Use the built-in scripted commands even if an LLM is configured
    #[arg(long)]
    pub no_llm: bool,
    /// Embedding model for long-term memory (OpenRouter; see `demo::llm::Embedder`)
    #[arg(long, env = "OPENROUTER_EMBED_MODEL")]
    pub embed_model: Option<String>,
    /// Disable long-term (LanceDB) memory; session history still applies
    #[arg(long)]
    pub no_memory: bool,
}

const REMOTE: &str = "remote";

pub struct Agent {
    pub name: String,
    persona: String,
    llm: Option<super::llm::Llm>,
    embedder: Option<super::llm::Embedder>,
    color: String,
    doc_id: String,
    speed: Duration,
    doc: Doc,
    text: TextRef,
    shapes: ArrayRef,
    chat: ArrayRef,
    aw: Arc<Mutex<Awareness>>,
    presence: Mutex<Value>,
    out: mpsc::UnboundedSender<Vec<u8>>,
    http: reqwest::Client,
    base: String,
    token: String,
    seq: std::sync::atomic::AtomicU64,
}

pub async fn run(args: AgentArgs) -> Result<()> {
    let color = args.color.clone().unwrap_or_else(|| match args.persona.as_str() {
        "sketch" => "#d9480f".into(),
        _ => "#7048e8".into(),
    });
    loop {
        match session(&args, &color).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::warn!(agent = %args.name, error = %e, "agent session ended; reconnecting in 2s");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn session(args: &AgentArgs, color: &str) -> Result<()> {
    let base = args.server.trim_end_matches('/').to_string();
    let ws_url = format!(
        "{}/ws/{}?token={}",
        base.replacen("http://", "ws://", 1).replacen("https://", "wss://", 1),
        args.doc,
        urlencode(&args.token)
    );
    let (ws, _) = tokio_tungstenite::connect_async(&ws_url).await.with_context(|| format!("connecting {}", args.doc))?;
    let (mut sink, mut stream) = ws.split();
    let (out, mut out_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    // UTF-16 offsets: the same indices the browser's Yjs uses.
    let doc = Doc::with_options(Options { offset_kind: OffsetKind::Utf16, ..Default::default() });
    let text = doc.get_or_insert_text("doc");
    let shapes = doc.get_or_insert_array("shapes");
    let chat = doc.get_or_insert_array("chat");
    let aw = Arc::new(Mutex::new(Awareness::new(doc.clone())));

    // Local edits -> server.
    let tx = out.clone();
    let _sub = doc
        .observe_update_v1(move |txn, e| {
            let remote = txn.origin().map(|o| o.as_ref() == REMOTE.as_bytes()).unwrap_or(false);
            if !remote {
                let _ = tx.send(Message::Sync(SyncMessage::Update(e.update.clone())).encode_v1());
            }
        })
        .map_err(|e| anyhow!("observe: {e:?}"))?;

    let writer = tokio::spawn(async move {
        while let Some(m) = out_rx.recv().await {
            if sink.send(WsMsg::Binary(m.into())).await.is_err() {
                break;
            }
        }
    });

    let synced = Arc::new(Notify::new());
    let reader = {
        let (doc, aw, out, synced) = (doc.clone(), aw.clone(), out.clone(), synced.clone());
        tokio::spawn(async move {
            let mut first = true;
            while let Some(Ok(msg)) = stream.next().await {
                let WsMsg::Binary(data) = msg else { continue };
                let mut dec = DecoderV1::new(Cursor::new(&data));
                for m in MessageReader::new(&mut dec).flatten() {
                    match m {
                        Message::Sync(SyncMessage::SyncStep1(sv)) => {
                            let u = doc.transact().encode_state_as_update_v1(&sv);
                            let _ = out.send(Message::Sync(SyncMessage::SyncStep2(u)).encode_v1());
                        }
                        Message::Sync(SyncMessage::SyncStep2(u)) | Message::Sync(SyncMessage::Update(u)) => {
                            if let Ok(update) = Update::decode_v1(&u) {
                                let _ = doc.transact_mut_with(REMOTE).apply_update(update);
                            }
                            if first {
                                first = false;
                                synced.notify_one();
                            }
                        }
                        Message::Awareness(u) => {
                            let _ = aw.lock().unwrap().apply_update(u);
                        }
                        _ => {}
                    }
                }
            }
        })
    };

    // Opening handshake: our state vector, so the server sends what we lack.
    let sv = doc.transact().state_vector();
    out.send(Message::Sync(SyncMessage::SyncStep1(sv)).encode_v1())?;
    if tokio::time::timeout(Duration::from_secs(10), synced.notified()).await.is_err() {
        bail!("no initial sync from server (permission?)");
    }

    let llm = if args.no_llm { None } else { super::llm::Llm::from_env(args.model.clone()) };
    match &llm {
        Some(l) => tracing::info!(agent = %args.name, model = %l.model, "agent brain: LLM"),
        None => tracing::info!(agent = %args.name, "agent brain: scripted commands (no OPENAI_COMPATIBLE_* configured)"),
    }
    let embedder = if args.no_memory || llm.is_none() { None } else { super::llm::Embedder::from_env(args.embed_model.clone()) };
    if llm.is_some() {
        match &embedder {
            Some(e) => tracing::info!(agent = %args.name, model = %e.model, "agent memory: session history (AgentFS) + long-term recall (LanceDB)"),
            None => tracing::info!(agent = %args.name, "agent memory: session history (AgentFS) only (no OPENROUTER_API_KEY)"),
        }
    }
    let agent = Arc::new(Agent {
        name: args.name.clone(),
        persona: args.persona.clone(),
        llm,
        embedder,
        color: color.to_string(),
        doc_id: args.doc.clone(),
        speed: Duration::from_millis(args.speed_ms.max(1)),
        doc,
        text,
        shapes,
        chat,
        aw,
        presence: Mutex::new(json!({})),
        out,
        http: reqwest::Client::new(),
        base,
        token: args.token.clone(),
        seq: Default::default(),
    });
    agent.set_presence(json!({
        "user": {"name": agent.name, "color": agent.color, "kind": "agent", "persona": args.persona},
        "status": "joined", "cursor": null, "pointer": null,
    }));
    tracing::info!(agent = %agent.name, persona = %args.persona, doc = %args.doc, "agent joined");

    // Presence heartbeat (browsers drop peers silent for 30s).
    let hb = {
        let a = agent.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                a.send_presence();
            }
        })
    };

    let behaviour = {
        let a = agent.clone();
        let persona = args.persona.clone();
        tokio::spawn(async move {
            let r = match persona.as_str() {
                "sketch" => sketch(a).await,
                _ => scribe(a).await,
            };
            if let Err(e) = r {
                tracing::error!(error = %e, "agent behaviour failed");
            }
        })
    };

    let _ = reader.await;
    behaviour.abort();
    hb.abort();
    writer.abort();
    bail!("connection closed")
}

fn urlencode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

// ---- agent primitives ----------------------------------------------------------------

impl Agent {
    fn set_presence(&self, patch: Value) {
        {
            let mut p = self.presence.lock().unwrap();
            if let (Some(obj), Some(patch)) = (p.as_object_mut(), patch.as_object()) {
                for (k, v) in patch {
                    obj.insert(k.clone(), v.clone());
                }
            }
        }
        self.send_presence();
    }

    fn send_presence(&self) {
        let state = self.presence.lock().unwrap().clone();
        let mut aw = self.aw.lock().unwrap();
        aw.set_local_state_raw(state.to_string());
        let me = aw.client_id();
        if let Ok(u) = aw.update_with_clients([me]) {
            let _ = self.out.send(Message::Awareness(u).encode_v1());
        }
    }

    fn status(&self, s: &str) {
        if s == "idle" {
            self.set_presence(json!({ "status": s, "pointer": null }));
        } else {
            self.set_presence(json!({ "status": s }));
        }
    }

    pub fn text(&self) -> String {
        self.text.get_string(&self.doc.transact())
    }

    /// Type `s` as a new paragraph near the end of the document, one character
    /// at a time. The agent first reserves its own line (an anchor newline) at
    /// the start of the last line, so a human who is mid-sentence at the end of
    /// the document is never split or interleaved, and types before that anchor
    /// through a sticky index that follows concurrent edits.
    async fn type_at_end(&self, s: &str) {
        self.type_paragraph(s, false).await
    }

    /// `by_word`: insert a word (with its whitespace) per step - used for
    /// longer LLM-written text so it streams in at reading speed.
    async fn type_paragraph(&self, s: &str, by_word: bool) {
        self.status("typing");
        let sticky = {
            let mut txn = self.doc.transact_mut();
            let text = self.text.get_string(&txn);
            let at = match text.rfind('\n') {
                Some(i) if i + 1 < text.len() => text[..=i].encode_utf16().count() as u32,
                _ => self.text.len(&txn),
            };
            self.text.insert(&mut txn, at, "\n");
            self.text.sticky_index(&txn, at, Assoc::After)
        };
        let body = s.trim_start_matches('\n').trim_end_matches('\n');
        let pieces: Vec<String> = if by_word {
            let mut v = vec![];
            let mut cur = String::new();
            for ch in body.chars() {
                cur.push(ch);
                if ch.is_whitespace() {
                    v.push(std::mem::take(&mut cur));
                }
            }
            if !cur.is_empty() {
                v.push(cur);
            }
            v
        } else {
            body.chars().map(|c| c.to_string()).collect()
        };
        for piece in pieces {
            let idx = {
                let mut txn = self.doc.transact_mut();
                let at = sticky
                    .as_ref()
                    .and_then(|st| st.get_offset(&txn))
                    .map(|o| o.index)
                    .unwrap_or_else(|| self.text.len(&txn));
                self.text.insert(&mut txn, at, &piece);
                at + piece.encode_utf16().count() as u32
            };
            self.set_presence(json!({ "cursor": { "index": idx } }));
            let pause = if piece.ends_with('\n') { self.speed * 4 } else if by_word { self.speed * 2 } else { self.speed };
            tokio::time::sleep(pause).await;
        }
        self.status("idle");
    }

    async fn say(&self, text: &str) {
        let n = self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let msg = json!({
            "id": format!("{}-{}-{n}", self.name, crate::agents::now_secs()),
            "author": self.name, "kind": "agent", "color": self.color,
            "text": text, "ts": now_ms(),
        });
        if let Ok(any) = Any::from_json(&msg.to_string()) {
            let mut txn = self.doc.transact_mut();
            self.chat.push_back(&mut txn, any);
        }
        self.post("/v1/transcripts", json!({"session_id": self.doc_id, "role": "assistant", "content": text})).await;
    }

    /// Record an action in the agent's own AgentFS tool-call log.
    async fn tool(&self, name: &str, params: Value, result: Value) {
        self.post(&format!("/v1/agents/{}/tools", self.name), json!({"name": name, "parameters": params, "result": result}))
            .await;
    }

    async fn post(&self, path: &str, body: Value) {
        let r = self.http.post(format!("{}{path}", self.base)).bearer_auth(&self.token).json(&body).send().await;
        match r {
            Ok(r) if !r.status().is_success() => {
                tracing::warn!(agent = %self.name, path, status = %r.status(), "request rejected")
            }
            Err(e) => tracing::warn!(agent = %self.name, path, error = %e, "request failed"),
            _ => {}
        }
    }

    /// Chat messages after `seen` (advances it).
    fn new_chat(&self, seen: &mut u32) -> Vec<Value> {
        let txn = self.doc.transact();
        let len = self.chat.len(&txn);
        let mut out = vec![];
        while *seen < len {
            if let Some(v) = self.chat.get(&txn, *seen) {
                let mut s = String::new();
                v.to_json(&txn).to_json(&mut s);
                if let Ok(j) = serde_json::from_str::<Value>(&s) {
                    out.push(j);
                }
            }
            *seen += 1;
        }
        out
    }

    /// `@name command args` addressed to this agent, from someone else.
    fn command(&self, msg: &Value) -> Option<(String, String, String)> {
        let author = msg.get("author")?.as_str()?.to_string();
        if author == self.name {
            return None;
        }
        let text = msg.get("text")?.as_str()?;
        let tag = format!("@{}", self.name.to_lowercase());
        let lower = text.to_lowercase();
        let at = lower.find(&tag)?;
        let rest = text[at + tag.len()..].trim().trim_start_matches([',', ':']).trim();
        let (cmd, arg) = rest.split_once(' ').unwrap_or((rest, ""));
        Some((author, cmd.to_lowercase(), arg.trim().to_string()))
    }

    fn participants(&self) -> Vec<String> {
        let aw = self.aw.lock().unwrap();
        let mut names: Vec<String> = aw
            .iter()
            .filter_map(|(_, st)| st.data.as_ref().and_then(|d| serde_json::from_str::<Value>(d).ok()))
            .filter_map(|v| v.pointer("/user/name").and_then(|n| n.as_str()).map(str::to_string))
            .collect();
        names.sort();
        names.dedup();
        names
    }

    // ---- drawing --------------------------------------------------------------------

    fn shapes_json(&self) -> Vec<Value> {
        let txn = self.doc.transact();
        let mut s = String::new();
        self.shapes.to_json(&txn).to_json(&mut s);
        serde_json::from_str::<Vec<Value>>(&s).unwrap_or_default()
    }

    fn new_shape(&self, kind: &str, x: f64, y: f64, label: &str) -> MapRef {
        let n = self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut txn = self.doc.transact_mut();
        self.shapes.push_back(
            &mut txn,
            MapPrelim::from([
                ("id", yrs::In::Any(Any::from(format!("{}-{}-{n}", self.name, now_ms())))),
                ("kind", Any::from(kind).into()),
                ("color", Any::from(self.color.as_str()).into()),
                ("author", Any::from(self.name.as_str()).into()),
                ("x", Any::from(x).into()),
                ("y", Any::from(y).into()),
                ("w", Any::from(0.0).into()),
                ("h", Any::from(0.0).into()),
                ("x2", Any::from(x).into()),
                ("y2", Any::from(y).into()),
                ("label", Any::from(label).into()),
                ("points", yrs::In::Array(ArrayPrelim::default())),
            ]),
        )
    }

    fn set_num(&self, m: &MapRef, fields: &[(&str, f64)]) {
        let mut txn = self.doc.transact_mut();
        for (k, v) in fields {
            m.insert(&mut txn, *k, Any::from(*v));
        }
    }

    fn pointer(&self, x: f64, y: f64) {
        self.set_presence(json!({ "pointer": {"x": x, "y": y} }));
    }

    /// Draw a box/ellipse/note by dragging its corner, with a visible pointer.
    async fn draw_box(&self, kind: &str, x: f64, y: f64, w: f64, h: f64, label: &str) -> MapRef {
        self.status("drawing");
        for i in 0..=6 {
            let t = i as f64 / 6.0;
            self.pointer(x + (x - 40.0) * (1.0 - t) * 0.1, y - 30.0 * (1.0 - t));
            tokio::time::sleep(self.speed).await;
        }
        let m = self.new_shape(kind, x, y, "");
        let steps = 12;
        for i in 1..=steps {
            let t = i as f64 / steps as f64;
            self.set_num(&m, &[("w", w * t), ("h", h * t)]);
            self.pointer(x + w * t, y + h * t);
            tokio::time::sleep(self.speed).await;
        }
        {
            let mut txn = self.doc.transact_mut();
            m.insert(&mut txn, "label", Any::from(label));
        }
        self.status("idle");
        m
    }

    async fn draw_arrow(&self, x: f64, y: f64, x2: f64, y2: f64) {
        self.status("drawing");
        let m = self.new_shape("arrow", x, y, "");
        let steps = 10;
        for i in 1..=steps {
            let t = i as f64 / steps as f64;
            let (cx, cy) = (x + (x2 - x) * t, y + (y2 - y) * t);
            self.set_num(&m, &[("x2", cx), ("y2", cy)]);
            self.pointer(cx, cy);
            tokio::time::sleep(self.speed).await;
        }
        self.status("idle");
    }

    /// Freehand stroke: points stream in, so everyone watches it being drawn.
    async fn draw_path(&self, pts: &[(f64, f64)]) {
        self.status("drawing");
        let Some(&(x0, y0)) = pts.first() else { return };
        let m = self.new_shape("pen", x0, y0, "");
        let arr = {
            let txn = self.doc.transact();
            match m.get(&txn, "points") {
                Some(Out::YArray(a)) => a,
                _ => return,
            }
        };
        for (x, y) in pts {
            {
                let mut txn = self.doc.transact_mut();
                arr.push_back(&mut txn, Any::from(*x));
                arr.push_back(&mut txn, Any::from(*y));
            }
            self.pointer(*x, *y);
            tokio::time::sleep(self.speed / 2).await;
        }
        self.status("idle");
    }

    fn remove_own_shapes(&self) -> usize {
        let mut txn = self.doc.transact_mut();
        let len = self.shapes.len(&txn);
        let mut removed = 0;
        for i in (0..len).rev() {
            let own = matches!(self.shapes.get(&txn, i), Some(Out::YMap(m)) if matches!(m.get(&txn, "author"), Some(Out::Any(Any::String(a))) if *a == *self.name));
            if own {
                self.shapes.remove(&mut txn, i);
                removed += 1;
            }
        }
        removed
    }
}

// ---- LLM brain --------------------------------------------------------------------

const MAX_ACTIONS: usize = 12;

fn system_prompt(name: &str, persona: &str) -> String {
    let role = match persona {
        "sketch" => "You are the team's visual thinker. You draw on the shared board: boxes, ellipses, \
                     sticky notes, arrows between labelled shapes, and freehand paths. You do not write \
                     in the document.",
        _ => "You are the team's scribe. You write clear, concise markdown into the shared document: \
              notes, summaries, decisions, plans. Use '## ' headings for new sections. You do not draw.",
    };
    format!(
        "You are {name}, an AI agent collaborating live with humans and other agents in enact-db: \
         a shared markdown document and a 1000x700 drawing board (origin top-left). {role}\n\n\
         Respond with ONLY a JSON object, no prose, no code fences:\n\
         {{\"reply\": \"short chat answer (max 40 words)\", \"actions\": [ ... ]}}\n\
         Action types (max {MAX_ACTIONS}):\n\
         - {{\"type\": \"write\", \"text\": \"markdown\"}}  appended to the document as your own paragraph\n\
         - {{\"type\": \"box\", \"label\": \"...\"}}  rectangle; also \"ellipse\" and \"note\" (sticky note)\n\
         - {{\"type\": \"arrow\", \"from\": \"label\", \"to\": \"label\"}}  between existing or just-drawn shapes\n\
         - {{\"type\": \"path\", \"points\": [[x,y], ...]}}  freehand stroke, 2-200 points inside 1000x700\n\
         - {{\"type\": \"remember\", \"text\": \"...\"}}  store a durable fact, decision or preference in your long-term memory\n\
         You have memory: earlier turns of this session are in the conversation, and relevant \
         long-term memories (from any session) are listed with each request. Use them for follow-ups \
         like \"make it shorter\" or \"what did we decide\". \
         Only use actions that fit your role (remember is for everyone). Keep labels under 24 characters. \
         If the request is not for you or needs nothing, return an empty actions list."
    )
}

impl Agent {
    /// Ask the LLM what to do about a request, then do it as a CRDT peer.
    async fn think(self: &Arc<Self>, who: &str, request: &str) {
        let Some(llm) = self.llm.clone() else { return };
        self.status("thinking");
        let started = crate::agents::now_secs();
        let system = system_prompt(&self.name, &self.persona);
        // Memory: session history from AgentFS (Turso), relevant long-term memories from LanceDB.
        let history = self.load_history(6).await;
        let memories = self.recall(request).await;
        let mut user = self.context(who, request);
        if !memories.is_empty() {
            user = format!(
                "Relevant long-term memories (most relevant first):\n{}\n\n{user}",
                memories.iter().map(|m| format!("- {m}")).collect::<Vec<_>>().join("\n")
            );
        }
        let mut messages = vec![json!({"role": "system", "content": system})];
        for t in &history {
            messages.push(json!({"role": "user", "content": format!("Request from {}: {}", t["who"].as_str().unwrap_or("?"), t["request"].as_str().unwrap_or(""))}));
            messages.push(json!({"role": "assistant", "content": json!({"reply": t["reply"], "actions": t["actions"]}).to_string()}));
        }
        messages.push(json!({"role": "user", "content": user}));
        let result = llm.chat(messages).await;
        let (raw, usage, latency) = match result {
            Ok(c) => (c.text, c.usage, c.latency_ms),
            Err(e) => {
                tracing::warn!(agent = %self.name, error = %e, "LLM call failed");
                self.status("idle");
                self.say(&format!("Sorry {who}, my model call failed ({}). Try again in a moment.", short(&e.to_string(), 120)))
                    .await;
                self.tool("llm.respond", json!({"requested_by": who, "request": request, "model": llm.model}), json!({"error": e.to_string()}))
                    .await;
                return;
            }
        };
        let parsed = super::llm::extract_json(&raw);
        let reply = parsed.as_ref().and_then(|v| v["reply"].as_str()).unwrap_or("").to_string();
        let actions: Vec<Value> = parsed
            .as_ref()
            .and_then(|v| v["actions"].as_array().cloned())
            .unwrap_or_default()
            .into_iter()
            .take(MAX_ACTIONS)
            .collect();
        if parsed.is_none() {
            self.say(&format!("Sorry {who}, I couldn't turn that into actions. Could you rephrase?")).await;
        } else if !reply.is_empty() {
            self.say(&short(&reply, 400)).await;
        }
        let mut done = vec![];
        for a in &actions {
            match self.act(a, who).await {
                Ok(d) => done.push(d),
                Err(e) => done.push(format!("skipped: {e}")),
            }
        }
        self.status("idle");

        // Session transcript (AgentFS) and long-term memory (LanceDB).
        let proposed = Value::Array(actions.clone());
        self.append_turn(json!({"ts": now_ms(), "who": who, "request": request, "reply": reply, "actions": proposed, "executed": done}))
            .await;
        if parsed.is_some() {
            let summary = format!("{who} asked: {request} | I replied: {reply} | I did: {}", done.join("; "));
            if let Err(e) = self.memorize(&short(&summary, 1500), "turn", who).await {
                tracing::debug!(agent = %self.name, error = %e, "turn not stored in long-term memory");
            }
        }

        // Everything about this exchange goes into the agent's own AgentFS sandbox.
        let record = json!({
            "ts": now_ms(), "agent": self.name, "persona": self.persona, "model": llm.model,
            "history_turns": history.len(), "memories": memories,
            "requested_by": who, "request": request, "system": system, "context": user,
            "raw_response": raw, "parsed": parsed, "executed": done, "usage": usage, "latency_ms": latency,
        });
        let path = format!("/v1/agents/{}/fs/llm/{}-{}.json", self.name, started, self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        let _ = self
            .http
            .put(format!("{}{path}", self.base))
            .bearer_auth(&self.token)
            .body(serde_json::to_vec_pretty(&record).unwrap_or_default())
            .send()
            .await;
        self.tool(
            "llm.respond",
            json!({"requested_by": who, "request": request, "model": llm.model}),
            json!({"reply": reply, "actions": done, "latency_ms": latency, "usage": usage}),
        )
        .await;
    }

    /// The live context the model sees.
    fn context(&self, who: &str, request: &str) -> String {
        const DOC_LIMIT: usize = 6000;
        let text = self.text();
        let doc = if text.chars().count() > DOC_LIMIT {
            let skip = text.chars().count() - DOC_LIMIT;
            format!("[...earlier text omitted...]\n{}", text.chars().skip(skip).collect::<String>())
        } else {
            text
        };
        let shapes: Vec<String> = self
            .shapes_json()
            .iter()
            .take(80)
            .map(|s| {
                format!(
                    "- {} \"{}\" by {} at ({:.0},{:.0}) size {:.0}x{:.0}",
                    s["kind"].as_str().unwrap_or("?"),
                    s["label"].as_str().unwrap_or(""),
                    s["author"].as_str().unwrap_or("?"),
                    s["x"].as_f64().unwrap_or(0.0),
                    s["y"].as_f64().unwrap_or(0.0),
                    s["w"].as_f64().unwrap_or(0.0).abs(),
                    s["h"].as_f64().unwrap_or(0.0).abs()
                )
            })
            .collect();
        let chat: Vec<String> = {
            let txn = self.doc.transact();
            let mut s = String::new();
            self.chat.to_json(&txn).to_json(&mut s);
            let all: Vec<Value> = serde_json::from_str(&s).unwrap_or_default();
            let n = all.len();
            all.into_iter()
                .skip(n.saturating_sub(15))
                .map(|m| format!("- {}: {}", m["author"].as_str().unwrap_or("?"), m["text"].as_str().unwrap_or("")))
                .collect()
        };
        format!(
            "Request from {who}: {request}\n\nParticipants online: {}\n\nShared document:\n<<<\n{doc}\n>>>\n\n\
             Board shapes:\n{}\n\nRecent chat:\n{}\n\nRespond with the JSON object only.",
            self.participants().join(", "),
            if shapes.is_empty() { "(empty)".to_string() } else { shapes.join("\n") },
            chat.join("\n")
        )
    }

    /// Execute one model-proposed action. Returns a short description.
    async fn act(&self, a: &Value, who: &str) -> Result<String> {
        let kind = a["type"].as_str().unwrap_or("").to_lowercase();
        let label = |k: &str| short(a[k].as_str().unwrap_or("").trim(), 40);
        match kind.as_str() {
            "write" => {
                let text = a["text"].as_str().unwrap_or("").trim();
                if text.is_empty() {
                    bail!("empty write");
                }
                let text = short(text, 3000);
                self.type_paragraph(&text, true).await;
                Ok(format!("write {} chars", text.chars().count()))
            }
            "box" | "rect" | "rectangle" | "ellipse" | "circle" | "note" | "sticky" => {
                let k = match kind.as_str() {
                    "ellipse" | "circle" => "ellipse",
                    "note" | "sticky" => "note",
                    _ => "rect",
                };
                let (x, y) = self.next_extra_slot();
                let (w, h) = if k == "ellipse" { (180.0, 100.0) } else if k == "note" { (180.0, 80.0) } else { (200.0, 80.0) };
                let l = label("label");
                self.draw_box(k, x, y, w, h, &l).await;
                Ok(format!("{k} \"{l}\""))
            }
            "arrow" => {
                let (from, to) = (label("from"), label("to"));
                let shapes = self.shapes_json();
                let find = |l: &str| {
                    shapes
                        .iter()
                        .rev()
                        .find(|s| s["label"].as_str().is_some_and(|x| x.eq_ignore_ascii_case(l)) && s["kind"] != "arrow")
                        .map(|s| {
                            let (x, y) = (s["x"].as_f64().unwrap_or(0.0), s["y"].as_f64().unwrap_or(0.0));
                            let (w, h) = (s["w"].as_f64().unwrap_or(0.0), s["h"].as_f64().unwrap_or(0.0));
                            (x.min(x + w), y.min(y + h), w.abs(), h.abs())
                        })
                };
                let (Some(a1), Some(b1)) = (find(&from), find(&to)) else { bail!("arrow: shape not found ({from} -> {to})") };
                let (p, q) = edge_points(a1, b1);
                self.draw_arrow(p.0, p.1, q.0, q.1).await;
                Ok(format!("arrow {from} -> {to}"))
            }
            "path" | "draw" | "pen" => {
                let pts: Vec<(f64, f64)> = a["points"]
                    .as_array()
                    .map(|v| {
                        v.iter()
                            .filter_map(|p| Some((p.get(0)?.as_f64()?.clamp(0.0, 1000.0), p.get(1)?.as_f64()?.clamp(0.0, 700.0))))
                            .take(200)
                            .collect()
                    })
                    .unwrap_or_default();
                if pts.len() < 2 {
                    bail!("path needs at least 2 points");
                }
                self.draw_path(&pts).await;
                Ok(format!("path {} points", pts.len()))
            }
            "remember" | "memorize" | "memory" => {
                let text = a["text"].as_str().or_else(|| a["fact"].as_str()).unwrap_or("").trim();
                if text.is_empty() {
                    bail!("empty remember");
                }
                if self.embedder.is_none() {
                    bail!("long-term memory is not configured");
                }
                self.memorize(&short(text, 1000), "fact", who).await?;
                Ok(format!("remembered \"{}\"", short(text, 60)))
            }
            other => bail!("unknown action '{other}'"),
        }
    }

    fn memory_namespace(&self) -> String {
        format!("agent-{}", self.name)
    }

    fn session_path(&self) -> String {
        format!("sessions/{}.jsonl", self.doc_id)
    }

    /// The last `n` turns of this session, from the agent's AgentFS (Turso).
    async fn load_history(&self, n: usize) -> Vec<Value> {
        let url = format!("{}/v1/agents/{}/fs/{}", self.base, self.name, self.session_path());
        let Ok(r) = self.http.get(url).bearer_auth(&self.token).send().await else { return vec![] };
        if !r.status().is_success() {
            return vec![];
        }
        let body = r.text().await.unwrap_or_default();
        let turns: Vec<Value> = body.lines().filter_map(|l| serde_json::from_str(l).ok()).collect();
        turns.into_iter().rev().take(n).collect::<Vec<_>>().into_iter().rev().collect()
    }

    /// Append one turn to the session transcript in AgentFS (the agent is its only writer).
    async fn append_turn(&self, turn: Value) {
        let url = format!("{}/v1/agents/{}/fs/{}", self.base, self.name, self.session_path());
        let mut body = match self.http.get(&url).bearer_auth(&self.token).send().await {
            Ok(r) if r.status().is_success() => r.bytes().await.map(|b| b.to_vec()).unwrap_or_default(),
            _ => vec![],
        };
        body.extend_from_slice(turn.to_string().as_bytes());
        body.push(b'\n');
        if let Err(e) = self.http.put(&url).bearer_auth(&self.token).body(body).send().await {
            tracing::warn!(agent = %self.name, error = %e, "session transcript not saved");
        }
    }

    /// Semantic search over this agent's long-term memory (LanceDB via /v1/memory).
    async fn recall(&self, query: &str) -> Vec<String> {
        let Some(emb) = &self.embedder else { return vec![] };
        let vector = match emb.embed_one(query).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(agent = %self.name, error = %e, "recall: embedding failed");
                return vec![];
            }
        };
        let r = self
            .http
            .post(format!("{}/v1/memory/search", self.base))
            .bearer_auth(&self.token)
            .json(&json!({"namespace": self.memory_namespace(), "embedding": vector, "k": 5}))
            .send()
            .await;
        let Ok(r) = r else { return vec![] };
        let v: Value = r.json().await.unwrap_or(Value::Null);
        v["hits"]
            .as_array()
            .map(|hits| {
                hits.iter()
                    .filter_map(|h| {
                        let content = h["content"].as_str()?;
                        let kind = h.pointer("/metadata/kind").and_then(|k| k.as_str()).unwrap_or("memory");
                        Some(format!("({kind}) {content}"))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Embed and store a memory (kind: "turn" or "fact") in LanceDB via /v1/memory.
    async fn memorize(&self, content: &str, kind: &str, who: &str) -> Result<()> {
        let emb = self.embedder.as_ref().ok_or_else(|| anyhow!("no embedder"))?;
        let vector = emb.embed_one(content).await?;
        let id = format!("{kind}-{}-{}", now_ms(), self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        let r = self
            .http
            .post(format!("{}/v1/memory", self.base))
            .bearer_auth(&self.token)
            .json(&json!({
                "namespace": self.memory_namespace(), "id": id, "content": content, "embedding": vector,
                "metadata": {"kind": kind, "who": who, "doc": self.doc_id, "ts": now_ms(), "model": emb.model},
            }))
            .send()
            .await?;
        if !r.status().is_success() {
            bail!("memory upsert: HTTP {}: {}", r.status(), r.text().await.unwrap_or_default());
        }
        Ok(())
    }

    /// Next free slot in this agent's "requested shapes" band (below the section flow).
    fn next_extra_slot(&self) -> (f64, f64) {
        let k = self
            .shapes_json()
            .iter()
            .filter(|s| s["author"] == self.name.as_str() && s["kind"] != "arrow" && s["kind"] != "pen" && s["y"].as_f64().unwrap_or(0.0) >= 380.0)
            .count();
        (40.0 + (k % 3) as f64 * 250.0, 400.0 + ((k / 3) % 2) as f64 * 130.0)
    }
}

/// Arrow endpoints on the borders of two boxes (x, y, w, h), along the line between centres.
fn edge_points(a: (f64, f64, f64, f64), b: (f64, f64, f64, f64)) -> ((f64, f64), (f64, f64)) {
    let ca = (a.0 + a.2 / 2.0, a.1 + a.3 / 2.0);
    let cb = (b.0 + b.2 / 2.0, b.1 + b.3 / 2.0);
    let (dx, dy) = (cb.0 - ca.0, cb.1 - ca.1);
    let border = |w: f64, h: f64| {
        let tx = if dx.abs() > 1e-6 { (w / 2.0) / dx.abs() } else { f64::INFINITY };
        let ty = if dy.abs() > 1e-6 { (h / 2.0) / dy.abs() } else { f64::INFINITY };
        tx.min(ty)
    };
    let ta = border(a.2 + 8.0, a.3 + 8.0);
    let tb = border(b.2 + 8.0, b.3 + 8.0);
    ((ca.0 + dx * ta, ca.1 + dy * ta), (cb.0 - dx * tb, cb.1 - dy * tb))
}

fn short(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        format!("{}…", s.chars().take(n).collect::<String>())
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Section headings ("## …") on complete lines. A heading still counts when
/// someone is mid-typing in front of it on the same line (a CRDT moment such
/// as "Bob: lau## Agenda").
fn headings(text: &str) -> Vec<String> {
    let complete = match text.rfind('\n') {
        Some(i) => &text[..i],
        None => "",
    };
    complete
        .lines()
        .filter_map(|l| {
            let i = l.find("## ")?;
            // "### x" is not a level-2 heading
            if i > 0 && l.as_bytes()[i - 1] == b'#' {
                return None;
            }
            Some(l[i + 3..].trim().to_string())
        })
        .filter(|h| !h.is_empty())
        .collect()
}

// ---- personas -------------------------------------------------------------------------

async fn scribe(a: Arc<Agent>) -> Result<()> {
    tokio::time::sleep(a.speed * 10).await;
    let mut seen = a.chat.len(&a.doc.transact());
    if !a.text().contains("## Agenda (scribe)") {
        if a.llm.is_some() {
            a.say("Hi all, I'm scribe (LLM). I'll keep the notes. Ask me anything with @scribe: summaries, plans, decisions, rewrites.").await;
        } else {
            a.say("Hi all, I'm scribe. I'll keep the notes. Ask me: \"@scribe summarize\" or \"@scribe write <text>\".").await;
        }
        a.type_at_end("\n## Agenda (scribe)\n- Goals for this session\n- Open questions\n- Next steps\n").await;
        a.tool("doc.write", json!({"doc": a.doc_id, "section": "Agenda"}), json!("ok")).await;
    }
    loop {
        for msg in a.new_chat(&mut seen) {
            let Some((who, cmd, arg)) = a.command(&msg) else { continue };
            if a.llm.is_some() {
                a.think(&who, msg["text"].as_str().unwrap_or_default()).await;
                continue;
            }
            match cmd.as_str() {
                "summarize" | "summary" | "summarise" => {
                    a.say(&format!("On it, {who}! Summarizing the document...")).await;
                    a.status("thinking");
                    tokio::time::sleep(a.speed * 8).await;
                    let text = a.text();
                    let hs: Vec<String> = headings(&text).into_iter().filter(|h| !h.starts_with("Summary")).collect();
                    let words = text.split_whitespace().count();
                    let people = a.participants().join(", ");
                    let block = format!(
                        "\n## Summary (scribe)\n- {} section{}: {}\n- {} words so far\n- Contributors: {}\n",
                        hs.len(),
                        if hs.len() == 1 { "" } else { "s" },
                        if hs.is_empty() { "none yet".into() } else { hs.join(", ") },
                        words,
                        people
                    );
                    a.type_at_end(&block).await;
                    a.say("Summary added to the document.").await;
                    a.tool("doc.summarize", json!({"requested_by": who}), json!({"sections": hs.len(), "words": words})).await;
                }
                "write" | "add" | "note" if !arg.is_empty() => {
                    a.say(&format!("Writing that down, {who}.")).await;
                    a.type_at_end(&format!("\n{arg}\n")).await;
                    a.tool("doc.write", json!({"requested_by": who, "text": arg}), json!("ok")).await;
                }
                _ => a.say("I can: \"@scribe summarize\", \"@scribe write <text>\".").await,
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn sketch(a: Arc<Agent>) -> Result<()> {
    tokio::time::sleep(a.speed * 25).await;
    let mut seen = a.chat.len(&a.doc.transact());
    if a.llm.is_some() {
        a.say("Hey! I'm sketch (LLM). I draw a box for every \"## section\" in the doc, and anything you describe with @sketch: flows, diagrams, notes, doodles.").await;
    } else {
        a.say("Hey! I'm sketch. I draw a box for every \"## section\" in the doc. Try \"@sketch box <label>\", \"@sketch circle <label>\", \"@sketch smile\" or \"@sketch clear\".").await;
    }

    // Layout: section boxes flow in a 3-column grid at the top (linked by
    // arrows); shapes requested in chat go in a separate band below, so
    // arrows never cross them.
    let slot = |k: usize| (40.0 + (k % 3) as f64 * 250.0, 70.0 + (k / 3) as f64 * 140.0);
    let extra_slot = |k: usize| (40.0 + (k % 3) as f64 * 250.0, 400.0 + ((k / 3) % 2) as f64 * 120.0);
    let mut extras = a.shapes_json().iter().filter(|s| s["author"] == a.name.as_str() && s["y"].as_f64().unwrap_or(0.0) >= 400.0).count();
    let mut drawn: HashSet<String> = a
        .shapes_json()
        .iter()
        .filter(|s| s["author"] == a.name.as_str() && s["kind"] == "rect")
        .filter_map(|s| s["label"].as_str().map(str::to_string))
        .collect();
    let mut placed = drawn.len();
    let mut last_box: Option<(f64, f64)> = None;
    if placed == 0 {
        a.draw_box("note", 790.0, 40.0, 180.0, 70.0, "Team board").await;
    }
    let mut pending: HashMap<String, Instant> = HashMap::new();
    let mut last_scan = Instant::now() - Duration::from_secs(5);

    loop {
        // Section boxes: draw each heading once it has been stable for a moment.
        if last_scan.elapsed() > Duration::from_millis(600) {
            last_scan = Instant::now();
            let hs = headings(&a.text());
            let fresh: Vec<String> = hs.iter().filter(|h| !drawn.contains(*h)).cloned().collect();
            for h in fresh.iter() {
                let first_seen = *pending.entry(h.clone()).or_insert_with(Instant::now);
                if first_seen.elapsed() < Duration::from_millis(900) {
                    continue;
                }
                let (x, y) = slot(placed);
                placed += 1;
                if let Some((px, py)) = last_box {
                    if (py - y).abs() < 1.0 {
                        a.draw_arrow(px + 200.0, py + 40.0, x - 4.0, y + 40.0).await; // same row: right edge -> left edge
                    } else {
                        a.draw_arrow(px + 100.0, py + 80.0, x + 100.0, y - 4.0).await; // next row: bottom -> top
                    }
                }
                a.draw_box("rect", x, y, 200.0, 80.0, h).await;
                a.tool("canvas.draw", json!({"kind": "rect", "label": h, "source": "doc heading"}), json!("ok")).await;
                last_box = Some((x, y));
                drawn.insert(h.clone());
            }
            pending.retain(|h, _| hs.contains(h));
        }

        for msg in a.new_chat(&mut seen) {
            let Some((who, cmd, arg)) = a.command(&msg) else { continue };
            if a.llm.is_some() {
                a.think(&who, msg["text"].as_str().unwrap_or_default()).await;
                continue;
            }
            let label = if arg.is_empty() { "idea".to_string() } else { arg.clone() };
            match cmd.as_str() {
                "box" | "rect" => {
                    a.say(&format!("Drawing a box \"{label}\" for {who}.")).await;
                    let (x, y) = extra_slot(extras);
                    extras += 1;
                    a.draw_box("rect", x, y, 200.0, 80.0, &label).await;
                    a.tool("canvas.draw", json!({"kind": "rect", "label": label, "requested_by": who}), json!("ok")).await;
                }
                "circle" | "ellipse" => {
                    a.say(&format!("Circle \"{label}\" coming up, {who}.")).await;
                    let (x, y) = extra_slot(extras);
                    extras += 1;
                    a.draw_box("ellipse", x, y, 180.0, 100.0, &label).await;
                    a.tool("canvas.draw", json!({"kind": "ellipse", "label": label, "requested_by": who}), json!("ok")).await;
                }
                "note" | "sticky" => {
                    let (x, y) = extra_slot(extras);
                    extras += 1;
                    a.draw_box("note", x, y, 180.0, 80.0, &label).await;
                    a.tool("canvas.draw", json!({"kind": "note", "label": label, "requested_by": who}), json!("ok")).await;
                }
                "smile" | "smiley" => {
                    a.say(&format!("Here's a smile for you, {who} :)")).await;
                    let (cx, cy, r) = (860.0, 560.0, 70.0);
                    let circle: Vec<(f64, f64)> = (0..=40)
                        .map(|i| {
                            let t = i as f64 / 40.0 * std::f64::consts::TAU;
                            (cx + r * t.cos(), cy + r * t.sin())
                        })
                        .collect();
                    a.draw_path(&circle).await;
                    a.draw_path(&[(cx - 25.0, cy - 20.0), (cx - 24.0, cy - 14.0)]).await;
                    a.draw_path(&[(cx + 25.0, cy - 20.0), (cx + 24.0, cy - 14.0)]).await;
                    let mouth: Vec<(f64, f64)> = (0..=16)
                        .map(|i| {
                            let t = 0.2 * std::f64::consts::PI + i as f64 / 16.0 * 0.6 * std::f64::consts::PI;
                            (cx + 40.0 * t.cos(), cy + 5.0 + 30.0 * t.sin())
                        })
                        .collect();
                    a.draw_path(&mouth).await;
                    a.tool("canvas.draw", json!({"kind": "pen", "label": "smile", "requested_by": who}), json!("ok")).await;
                }
                "diagram" => {
                    a.say("Redrawing the section diagram.").await;
                    drawn.clear();
                    last_box = None;
                    placed = 0;
                    extras = 0;
                    a.remove_own_shapes();
                }
                "clear" => {
                    let n = a.remove_own_shapes();
                    drawn.clear();
                    last_box = None;
                    placed = 0;
                    extras = 0;
                    a.say(&format!("Cleared my {n} shapes.")).await;
                    a.tool("canvas.clear", json!({"requested_by": who}), json!({"removed": n})).await;
                }
                _ => a.say("I can draw: \"@sketch box <label>\", \"circle <label>\", \"note <text>\", \"smile\", \"diagram\", \"clear\".").await,
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_endpoints_sit_on_box_borders() {
        let (p, q) = edge_points((0.0, 0.0, 200.0, 80.0), (300.0, 0.0, 200.0, 80.0));
        assert_eq!((p.0.round(), p.1), (204.0, 40.0));
        assert_eq!((q.0.round(), q.1), (296.0, 40.0));
    }

    #[test]
    fn heading_detection_ignores_partial_last_line() {
        assert_eq!(headings("## A\ntext\n## B\n## Parti"), vec!["A", "B"]);
        assert!(headings("## Typing").is_empty());
        assert_eq!(headings("Bob: lau## Agenda\n### Sub\n"), vec!["Agenda"]);
    }
}
