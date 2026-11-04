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
}

const REMOTE: &str = "remote";

pub struct Agent {
    pub name: String,
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

    let agent = Arc::new(Agent {
        name: args.name.clone(),
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
        for ch in s.trim_start_matches('\n').trim_end_matches('\n').chars() {
            let idx = {
                let mut txn = self.doc.transact_mut();
                let at = sticky
                    .as_ref()
                    .and_then(|st| st.get_offset(&txn))
                    .map(|o| o.index)
                    .unwrap_or_else(|| self.text.len(&txn));
                self.text.insert(&mut txn, at, &ch.to_string());
                at + ch.len_utf16() as u32
            };
            self.set_presence(json!({ "cursor": { "index": idx } }));
            tokio::time::sleep(if ch == '\n' { self.speed * 4 } else { self.speed }).await;
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
        a.say("Hi all, I'm scribe. I'll keep the notes. Ask me: \"@scribe summarize\" or \"@scribe write <text>\".").await;
        a.type_at_end("\n## Agenda (scribe)\n- Goals for this session\n- Open questions\n- Next steps\n").await;
        a.tool("doc.write", json!({"doc": a.doc_id, "section": "Agenda"}), json!("ok")).await;
    }
    loop {
        for msg in a.new_chat(&mut seen) {
            let Some((who, cmd, arg)) = a.command(&msg) else { continue };
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
    a.say("Hey! I'm sketch. I draw a box for every \"## section\" in the doc. Try \"@sketch box <label>\", \"@sketch circle <label>\", \"@sketch smile\" or \"@sketch clear\".").await;

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
    fn heading_detection_ignores_partial_last_line() {
        assert_eq!(headings("## A\ntext\n## B\n## Parti"), vec!["A", "B"]);
        assert!(headings("## Typing").is_empty());
        assert_eq!(headings("Bob: lau## Agenda\n### Sub\n"), vec!["Agenda"]);
    }
}
