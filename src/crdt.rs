//! Multiplayer CRDT hub: humans (browser, Yjs + y-websocket) and agents (HTTP or
//! Yjs/yrs/pycrdt over WebSocket) edit the same documents concurrently.
//!
//! * Wire protocol: standard y-protocols (sync step1/step2/update + awareness),
//!   so the stock `y-websocket` WebsocketProvider works unchanged.
//! * Every update is fanned out to all other peers in the room.
//! * Every update is appended to a Turso log (`<data_dir>/hub.db`) and the log is
//!   periodically compacted into a single snapshot; documents are rebuilt from
//!   the log on first access, so they survive restarts.

use crate::store::HubStore;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use yrs::encoding::read::Cursor;
use yrs::sync::protocol::{MessageReader, SyncMessage};
use yrs::sync::{Awareness, AwarenessUpdate, Message};
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::Encode;
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};

/// A frame broadcast to all peers of a room. `origin` lets the sender skip its own echo.
#[derive(Clone, Debug)]
pub struct Frame {
    pub origin: Arc<str>,
    pub bytes: Arc<Vec<u8>>,
}

pub struct Handled {
    pub replies: Vec<Vec<u8>>,
    /// Awareness client ids seen in this frame (to clean up on disconnect).
    pub awareness_clients: Vec<yrs::ClientID>,
}

pub struct Room {
    pub id: String,
    pub awareness: Mutex<Awareness>,
    pub tx: broadcast::Sender<Frame>,
    _subs: Vec<yrs::Subscription>,
}

impl Room {
    pub fn doc(&self) -> Doc {
        self.awareness.lock().unwrap().doc().clone()
    }

    /// The opening messages a server sends a new peer: SyncStep1 (our state
    /// vector, so the peer sends what we lack) + SyncStep2 (everything we have,
    /// so the peer catches up immediately) + current awareness.
    pub fn handshake(&self) -> Result<Vec<Vec<u8>>> {
        let aw = self.awareness.lock().unwrap();
        let txn = aw.doc().transact();
        let sv = txn.state_vector();
        let full = txn.encode_state_as_update_v1(&StateVector::default());
        drop(txn);
        let mut out = vec![
            Message::Sync(SyncMessage::SyncStep1(sv)).encode_v1(),
            Message::Sync(SyncMessage::SyncStep2(full)).encode_v1(),
        ];
        if let Ok(update) = aw.update() {
            if !update.clients.is_empty() {
                out.push(Message::Awareness(update).encode_v1());
            }
        }
        Ok(out)
    }

    /// Handle one binary y-protocol frame from a peer. Returns direct replies
    /// for that peer; doc/awareness changes are broadcast by the observers.
    pub fn handle(&self, origin: &str, data: &[u8]) -> Result<Handled> {
        let mut replies = vec![];
        let mut aw_clients = vec![];
        let mut decoder = DecoderV1::new(Cursor::new(data));
        let reader = MessageReader::new(&mut decoder);
        for msg in reader {
            let msg = msg.map_err(|e| anyhow!("bad y-protocol frame: {e}"))?;
            let mut aw = self.awareness.lock().unwrap();
            match msg {
                Message::Sync(SyncMessage::SyncStep1(sv)) => {
                    let update = aw.doc().transact().encode_state_as_update_v1(&sv);
                    replies.push(Message::Sync(SyncMessage::SyncStep2(update)).encode_v1());
                }
                Message::Sync(SyncMessage::SyncStep2(bytes)) | Message::Sync(SyncMessage::Update(bytes)) => {
                    let update = Update::decode_v1(&bytes).map_err(|e| anyhow!("bad update: {e}"))?;
                    let mut txn = aw.doc().transact_mut_with(origin.to_string());
                    txn.apply_update(update).map_err(|e| anyhow!("apply update: {e}"))?;
                }
                Message::AwarenessQuery => {
                    if let Ok(u) = aw.update() {
                        replies.push(Message::Awareness(u).encode_v1());
                    }
                }
                Message::Awareness(update) => {
                    aw_clients.extend(update.clients.keys().copied());
                    aw.apply_update_with(update, origin.to_string())
                        .map_err(|e| anyhow!("awareness: {e}"))?;
                }
                Message::Auth(_) | Message::Custom(..) => {}
            }
        }
        Ok(Handled { replies, awareness_clients: aw_clients })
    }

    /// Drop presence entries a disconnected peer introduced (broadcasts removal).
    pub fn remove_presence(&self, clients: &[yrs::ClientID]) {
        let mut aw = self.awareness.lock().unwrap();
        for c in clients {
            aw.remove_state(*c);
        }
    }

    /// Apply a raw Yjs v1 update (e.g. produced by an agent's local Yjs/pycrdt doc).
    pub fn apply_update(&self, origin: &str, bytes: &[u8]) -> Result<()> {
        let update = Update::decode_v1(bytes).map_err(|e| anyhow!("bad update: {e}"))?;
        let aw = self.awareness.lock().unwrap();
        let mut txn = aw.doc().transact_mut_with(origin.to_string());
        txn.apply_update(update).map_err(|e| anyhow!("apply update: {e}"))?;
        Ok(())
    }

    /// Publish presence for a non-websocket participant (e.g. an HTTP agent),
    /// so humans see "agent X is editing" in their editor.
    pub fn set_presence(&self, origin: &str, client_id: u64, state: Option<serde_json::Value>) -> Result<()> {
        let client_id = yrs::ClientID::new(client_id);
        let mut aw = self.awareness.lock().unwrap();
        let clock = aw.meta(client_id).map(|(c, _)| c + 1).unwrap_or(1);
        let json: Arc<str> = match state {
            Some(v) => v.to_string().into(),
            None => "null".into(),
        };
        let mut clients = HashMap::new();
        clients.insert(client_id, yrs::sync::awareness::AwarenessUpdateEntry { clock, json });
        aw.apply_update_with(AwarenessUpdate { clients }, origin.to_string())
            .map_err(|e| anyhow!("awareness: {e}"))?;
        Ok(())
    }

    pub fn full_state(&self) -> Vec<u8> {
        self.doc().transact().encode_state_as_update_v1(&StateVector::default())
    }

    pub fn diff(&self, sv: &[u8]) -> Result<Vec<u8>> {
        let sv = StateVector::decode_v1(sv).map_err(|e| anyhow!("bad state vector: {e}"))?;
        Ok(self.doc().transact().encode_state_as_update_v1(&sv))
    }

    pub fn peer_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

pub struct Hub {
    store: Arc<HubStore>,
    rooms: tokio::sync::Mutex<HashMap<String, Arc<Room>>>,
}

pub fn valid_doc_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 200
        && id.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'))
}

impl Hub {
    pub fn new(store: Arc<HubStore>) -> Self {
        Self { store, rooms: tokio::sync::Mutex::new(HashMap::new()) }
    }

    pub async fn loaded_rooms(&self) -> Vec<Arc<Room>> {
        self.rooms.lock().await.values().cloned().collect()
    }

    /// Get or load a room. Loading replays the persisted log before any
    /// observer is attached, so replayed updates are not re-persisted.
    pub async fn room(&self, id: &str) -> Result<Arc<Room>> {
        if !valid_doc_id(id) {
            return Err(anyhow!("invalid document id '{id}'"));
        }
        let mut rooms = self.rooms.lock().await;
        if let Some(r) = rooms.get(id) {
            return Ok(r.clone());
        }
        let doc = Doc::new();
        let updates = self.store.load_doc(id).await?;
        {
            let mut txn = doc.transact_mut();
            for u in &updates {
                match Update::decode_v1(u) {
                    Ok(update) => {
                        if let Err(e) = txn.apply_update(update) {
                            tracing::error!(doc = id, error = %e, "corrupt persisted update skipped");
                        }
                    }
                    Err(e) => tracing::error!(doc = id, error = %e, "undecodable persisted update skipped"),
                }
            }
        }
        tracing::info!(doc = id, replayed = updates.len(), "loaded document");

        let (tx, _) = broadcast::channel::<Frame>(1024);
        let mut awareness = Awareness::new(doc.clone());

        // Doc changes -> broadcast to peers + append to the persistent log.
        let doc_sub = {
            let tx = tx.clone();
            let store = self.store.clone();
            let doc_id = id.to_string();
            let snapshot_doc = doc.clone();
            doc.observe_update_v1(move |txn, ev| {
                let origin: Arc<str> = txn
                    .origin()
                    .map(|o| String::from_utf8_lossy(o.as_ref()).into_owned())
                    .unwrap_or_else(|| "server".into())
                    .into();
                let msg = Message::Sync(SyncMessage::Update(ev.update.clone())).encode_v1();
                let _ = tx.send(Frame { origin: origin.clone(), bytes: Arc::new(msg) });
                store.append(&doc_id, ev.update.clone(), origin.to_string(), snapshot_doc.clone());
            })
            .map_err(|e| anyhow!("observe doc: {e:?}"))?
        };

        // Awareness changes -> broadcast only the changed clients.
        let aw_sub = {
            let tx = tx.clone();
            awareness.on_update(move |aw, ev, origin| {
                let changed = ev.all_changes();
                if let Ok(update) = aw.update_with_clients(changed) {
                    let origin: Arc<str> = origin
                        .map(|o| String::from_utf8_lossy(o.as_ref()).into_owned())
                        .unwrap_or_else(|| "server".into())
                        .into();
                    let _ = tx.send(Frame { origin, bytes: Arc::new(Message::Awareness(update).encode_v1()) });
                }
            })
        };

        let room = Arc::new(Room {
            id: id.to_string(),
            awareness: Mutex::new(awareness),
            tx,
            _subs: vec![doc_sub, aw_sub],
        });
        rooms.insert(id.to_string(), room.clone());
        Ok(room)
    }
}
