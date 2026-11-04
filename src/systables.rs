//! `gateway.system.*` – the gateway's live state exposed as SQL tables so it
//! can be joined with enterprise sources:
//!
//! ```sql
//! SELECT t.agent_id, t.name, t.status, c.region
//! FROM system.agent_tool_calls t JOIN customers c ON c.owner = t.agent_id;
//! ```

use crate::agents::AgentRegistry;
use crate::catalog::{LiveTable, Loader};
use crate::crdt::Hub;
use crate::store::HubStore;
use anyhow::Result;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::MemorySchemaProvider;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

fn s(v: Vec<Option<String>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn i(v: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(v))
}

fn schema(fields: &[(&str, DataType)]) -> Arc<Schema> {
    Arc::new(Schema::new(fields.iter().map(|(n, t)| Field::new(*n, t.clone(), true)).collect::<Vec<_>>()))
}

pub fn register(ctx: &SessionContext, agents: Arc<AgentRegistry>, hub: Arc<Hub>, store: Arc<HubStore>) -> Result<()> {
    let catalog = ctx.catalog("gateway").expect("default catalog");
    catalog.register_schema("system", Arc::new(MemorySchemaProvider::new()))?;

    // agent_tool_calls -------------------------------------------------------------
    let sch = schema(&[
        ("agent_id", DataType::Utf8),
        ("id", DataType::Int64),
        ("name", DataType::Utf8),
        ("parameters", DataType::Utf8),
        ("result", DataType::Utf8),
        ("error", DataType::Utf8),
        ("status", DataType::Utf8),
        ("started_at", DataType::Int64),
        ("completed_at", DataType::Int64),
        ("duration_ms", DataType::Int64),
    ]);
    let (a, sc) = (agents.clone(), sch.clone());
    let loader: Loader = Arc::new(move || {
        let (a, sc) = (a.clone(), sc.clone());
        Box::pin(async move {
            let rows = a.all_tool_calls(10_000).await?;
            Ok(RecordBatch::try_new(
                sc,
                vec![
                    s(rows.iter().map(|(id, _)| Some(id.clone())).collect()),
                    i(rows.iter().map(|(_, t)| Some(t.id)).collect()),
                    s(rows.iter().map(|(_, t)| Some(t.name.clone())).collect()),
                    s(rows.iter().map(|(_, t)| t.parameters.as_ref().map(|v| v.to_string())).collect()),
                    s(rows.iter().map(|(_, t)| t.result.as_ref().map(|v| v.to_string())).collect()),
                    s(rows.iter().map(|(_, t)| t.error.clone()).collect()),
                    s(rows.iter().map(|(_, t)| Some(t.status.to_string())).collect()),
                    i(rows.iter().map(|(_, t)| Some(t.started_at)).collect()),
                    i(rows.iter().map(|(_, t)| t.completed_at).collect()),
                    i(rows.iter().map(|(_, t)| t.duration_ms).collect()),
                ],
            )?)
        })
    });
    ctx.register_table("system.agent_tool_calls", Arc::new(LiveTable::new(sch, loader)))?;

    // agent_kv -----------------------------------------------------------------------
    let sch = schema(&[("agent_id", DataType::Utf8), ("key", DataType::Utf8), ("value", DataType::Utf8)]);
    let (a, sc) = (agents.clone(), sch.clone());
    let loader: Loader = Arc::new(move || {
        let (a, sc) = (a.clone(), sc.clone());
        Box::pin(async move {
            let rows = a.all_kv().await?;
            Ok(RecordBatch::try_new(
                sc,
                vec![
                    s(rows.iter().map(|r| Some(r.0.clone())).collect()),
                    s(rows.iter().map(|r| Some(r.1.clone())).collect()),
                    s(rows.iter().map(|r| Some(r.2.clone())).collect()),
                ],
            )?)
        })
    });
    ctx.register_table("system.agent_kv", Arc::new(LiveTable::new(sch, loader)))?;

    // crdt_docs -----------------------------------------------------------------------
    let sch = schema(&[
        ("doc_id", DataType::Utf8),
        ("log_entries", DataType::Int64),
        ("bytes", DataType::Int64),
        ("last_update", DataType::Int64),
        ("live", DataType::Boolean),
        ("peers", DataType::Int64),
    ]);
    let (h, st, sc) = (hub.clone(), store.clone(), sch.clone());
    let loader: Loader = Arc::new(move || {
        let (h, st, sc) = (h.clone(), st.clone(), sc.clone());
        Box::pin(async move {
            let stats = st.doc_stats().await?;
            let live: std::collections::HashMap<String, usize> =
                h.loaded_rooms().await.into_iter().map(|r| (r.id.clone(), r.peer_count())).collect();
            Ok(RecordBatch::try_new(
                sc,
                vec![
                    s(stats.iter().map(|d| Some(d.doc_id.clone())).collect()),
                    i(stats.iter().map(|d| Some(d.log_entries)).collect()),
                    i(stats.iter().map(|d| Some(d.bytes)).collect()),
                    i(stats.iter().map(|d| Some(d.last_update)).collect()),
                    Arc::new(BooleanArray::from(stats.iter().map(|d| Some(live.contains_key(&d.doc_id))).collect::<Vec<_>>())),
                    i(stats.iter().map(|d| Some(live.get(&d.doc_id).copied().unwrap_or(0) as i64)).collect()),
                ],
            )?)
        })
    });
    ctx.register_table("system.crdt_docs", Arc::new(LiveTable::new(sch, loader)))?;

    // memories (local Turso store) ---------------------------------------------------------
    let sch = schema(&[
        ("namespace", DataType::Utf8),
        ("id", DataType::Utf8),
        ("content", DataType::Utf8),
        ("metadata", DataType::Utf8),
        ("agent_id", DataType::Utf8),
        ("created_at", DataType::Int64),
    ]);
    let (st, sc) = (store.clone(), sch.clone());
    let loader: Loader = Arc::new(move || {
        let (st, sc) = (st.clone(), sc.clone());
        Box::pin(async move {
            let rows = st.memory_all().await?;
            Ok(RecordBatch::try_new(
                sc,
                vec![
                    s(rows.iter().map(|m| Some(m.namespace.clone())).collect()),
                    s(rows.iter().map(|m| Some(m.id.clone())).collect()),
                    s(rows.iter().map(|m| Some(m.content.clone())).collect()),
                    s(rows.iter().map(|m| m.metadata.as_ref().map(|v| v.to_string())).collect()),
                    s(rows.iter().map(|m| m.agent_id.clone()).collect()),
                    i(rows.iter().map(|m| Some(m.created_at)).collect()),
                ],
            )?)
        })
    });
    ctx.register_table("system.local_memories", Arc::new(LiveTable::new(sch, loader)))?;

    // transcripts (local outbox) -------------------------------------------------------------
    let sch = schema(&[
        ("seq", DataType::Int64),
        ("session_id", DataType::Utf8),
        ("role", DataType::Utf8),
        ("actor", DataType::Utf8),
        ("content", DataType::Utf8),
        ("metadata", DataType::Utf8),
        ("created_at", DataType::Int64),
    ]);
    let (st, sc) = (store, sch.clone());
    let loader: Loader = Arc::new(move || {
        let (st, sc) = (st.clone(), sc.clone());
        Box::pin(async move {
            let rows = st.transcripts_after(0, 1_000_000, None).await?;
            Ok(RecordBatch::try_new(
                sc,
                vec![
                    i(rows.iter().map(|e| Some(e.seq)).collect()),
                    s(rows.iter().map(|e| Some(e.session_id.clone())).collect()),
                    s(rows.iter().map(|e| Some(e.role.clone())).collect()),
                    s(rows.iter().map(|e| e.actor.clone()).collect()),
                    s(rows.iter().map(|e| Some(e.content.clone())).collect()),
                    s(rows.iter().map(|e| e.metadata.as_ref().map(|v| v.to_string())).collect()),
                    i(rows.iter().map(|e| Some(e.created_at)).collect()),
                ],
            )?)
        })
    });
    ctx.register_table("system.transcripts", Arc::new(LiveTable::new(sch, loader)))?;
    Ok(())
}
