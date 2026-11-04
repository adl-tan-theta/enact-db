# enact-db: status and next steps

*Last updated: 2026-11-04 (commit after `e0fb6f5`).*

enact-db is one Rust binary (about 10k lines in `src/`) for **multiplayer AI
workspaces**: humans in browsers and AI agents work on the same live state.
Memory, transcripts, permissions and enterprise data all sit behind one endpoint.
This page records what is built, where each kind of data lives, how each part
was verified, and what comes next. The [README](../README.md) has the full API
and configuration reference.

## 1. What is built

| Area | What it does | Code |
|---|---|---|
| **CRDT hub** | Yjs-compatible rooms (yrs 0.27). It speaks the y-websocket sync and awareness protocol, persists an update log with compaction, has read-only peers, and offers HTTP ops on text, maps and arrays. | `src/crdt.rs`, `src/store.rs` |
| **AgentFS sandboxes** | One Turso file per agent, using the real `agentfs-sdk`. Each has files, KV and a tool-call log, and is reachable over HTTP only, so there are no file locks. | `src/agents.rs` |
| **Memory** | Vector memory with three backends: Turso (local), pgvector (cloud) and LanceDB (local). Namespaced and permission-checked. | `src/memory.rs` |
| **Transcripts** | A local outbox in `hub.db`, exported to DuckDB (local) and Postgres (cloud) with one cursor per sink. | `src/transcripts.rs` |
| **Blob tier** | `object_store` (Azure Blob or `file://`). Cloud sandbox agents push and pull their AgentFS file, and artifacts are stored there too. | `src/blob.rs` |
| **SQL federation** | DataFusion 55 over local, Azure and S3 files (Parquet, CSV, JSON), Postgres, Databricks (SQL Statement Execution API), Snowflake (ADBC), Iceberg REST catalogs, and the live `system.*` tables (agent tool calls, KV, CRDT docs, memories, transcripts, audit log, grants). | `src/catalog.rs`, `src/databricks.rs`, `src/iceberg/`, `src/systables.rs` |
| **Quack** | The DuckDB client-server protocol, v3. DuckDB clients can `ATTACH` the gateway. It supports catalog emulation, PREPARE/FETCH batching, and LIST/STRUCT/DECIMAL types. Read-only. | `src/quack/` |
| **Iceberg** | `[[iceberg]]` mounts a REST catalog (Lakekeeper, Polaris, Unity) as a DataFusion catalog. It has its own TableProvider, storage through `object_store`, and honours credentials vended by the catalog. | `src/iceberg/` |
| **Permissions** | Lakekeeper's model: principals `user:`, `agent:` and `role:`; grants on a resource tree that are inherited downward; the privilege chain `ownership ⇒ modify ⇒ write_data ⇒ select ⇒ describe`, plus `create`, `manage_grants`, `pass_grants` and `assume`. Also OIDC JWTs with admission gates, `edb_…` API tokens (stored hashed), a break-glass admin token, a versioned audit log, and table checks on the SQL plan (subqueries included). | `src/authz/` |
| **Multiplayer demo** | `enact-db demo` sets up a team through the authz API: alice and bob (editors), carol (viewer), and the agents scribe (writes) and sketch (draws). The page has a shared editor with live cursors, a vector board, chat, per-user undo and a SQL activity feed. | `src/demo/mod.rs`, `src/web/demo.html` |
| **LLM agents** | Any OpenAI-compatible chat endpoint; the default is MiniMax-M3 with `max_tokens` 100000. The model answers with JSON actions: `write`, `box`, `ellipse`, `note`, `arrow`, `path` and `remember`. The agents are real CRDT peers with sticky cursors and their own paragraphs. | `src/demo/agent.rs`, `src/demo/llm.rs` |
| **Agent memory** | Session history: the last 6 turns from AgentFS `sessions/<doc>.jsonl`, sent to the model as chat history. Long-term memory: embeddings through OpenRouter, stored in LanceDB under the agent's private namespace `agent-<name>`, with the top 5 recalled into each prompt. | `src/demo/agent.rs` |
| **Web pages** | `/` is a workspace with an editor, a SQL activity feed and "ask an agent". `/demo` is the multiplayer page. Both use a bundled Yjs client. | `src/web/`, `web/` |

## 2. Where the data lives

| Data | Local (default) | Cloud (when configured) |
|---|---|---|
| Shared documents (CRDT log) | `data/hub.db` (Turso) | `hub.db` travels with the server |
| Agent sandboxes: files, KV, tool calls, session transcripts, LLM exchange records | `data/agents/<agent>.db` (AgentFS / Turso) | Azure Blob (`--pull-agent` / push) |
| Vector memory | Turso or LanceDB (`data/lancedb/<embedding-model>`) | Postgres + pgvector |
| Chat / session transcripts (export) | DuckDB | Postgres |
| Artifacts | `file://` blob store | Azure Blob |
| Permissions, tokens, audit log | `data/authz.db` | — |
| Enterprise data (read through SQL) | local files | Postgres, Databricks, Snowflake, Iceberg, Azure/S3 files |

API keys (`OPENAI_COMPATIBLE_API_KEY`, `OPENROUTER_API_KEY`, catalog and
warehouse tokens) are read from the environment or config only. They are never
written to records or logs, and a test checks this.

## 3. How each part was verified

**Against real services**

| Service | Test |
|---|---|
| Postgres and pgvector (sources, memory, transcript sink) | `tests/backends.rs` with `ENACT_DB_TEST_PG` |
| DuckDB transcript sink, LanceDB memory | `tests/backends.rs` (`--features full`) |
| Two or three Chromium users plus agents co-editing and drawing; read-only viewer; per-user undo; convergence across all replicas and the server | `tests/browser.mjs`, `tests/demo.mjs` |
| MiniMax-M3 driving the agents, including a follow-up that relies on session history | `tests/demo_llm.mjs` (live) |
| MiniMax-M3, OpenRouter embeddings and LanceDB: a fact is remembered, the process restarts, and the fact is recalled on a different document. The test checks that it reached the prompt only through LanceDB. | `tests/memory_live.mjs` (live) |
| Quack with a real DuckDB client | tested by the project owner. In CI, `tests/quack.rs` decodes the wire format with a decoder written independently from DuckDB's C++ sources. |

**Against faithful mocks** (deterministic, run in CI)

| Area | Test |
|---|---|
| Databricks Statement Execution API: polling, multi-chunk Arrow, pushdown, token isolation | `tests/databricks.rs` |
| Iceberg: real Iceberg table files served by a spec-shaped REST catalog with a bearer token; pushdown; a new snapshot mid-test; access over Quack | `tests/iceberg.rs` |
| OIDC admission, roles, tokens, `assume`, `pass_grants`, subquery smuggling, read-only WebSocket peers, audit | `tests/authz.rs` |
| LLM actions, `<think>` stripping, invalid actions skipped, records stored | `tests/llm_agents.rs` |
| Session history, `remember`, private memory (403), recall after a restart. The embeddings mock accepts single strings only, as the free NVIDIA models do. | `tests/agent_memory.rs` |
| Everything else end to end: auth, AgentFS, KV, SQL federation, presence, Turso memory, transcripts, blob, restart persistence, compaction, sandbox push → wipe → pull | `tests/e2e.rs` |

**Not yet run against the live service**

| Service | Status |
|---|---|
| Azure Blob | Same `object_store` code path as `file://`, plus `az://` credentials |
| Snowflake (ADBC) | No test |
| A live Databricks workspace | Tested against the mock only |
| A live Lakekeeper, Polaris or Unity server | Tested against the mock REST catalog only |

**Embedding models tried** through OpenRouter:

| Model | Dimensions | Notes |
|---|---|---|
| `nvidia/nemotron-3.5-embed-1b:free` (default) | 2048 | One text per request: batches get HTTP 502. The client sends single strings and falls back from batches. |
| `nvidia/nemotron-3.5-embed-8b:free` | 4096 | One text per request |
| `amazon/titan-embed-text-v1` | 1536 | Batches work. Similarity was 0.63 for a related pair and 0.04 for an unrelated one. |

**Known upstream issue:** in turso 0.4.4, an index on `(x, rowid_alias)` makes
`x = ? AND rowid_alias > ?` return no rows. `src/store.rs` works around it
with `+seq`. This is worth reporting upstream.

## 4. Next steps

In suggested order. Nothing below is started yet.

### 4.1 Catalog UI (`/catalog`) — not built

There is no catalog browser today. Catalog access works through SQL
(`information_schema`, filtered by permissions), the Quack catalog that DuckDB
sees on `ATTACH`, and the `/v1/authz` API. Plan: one page in the binary, in the
same style as `/demo`, built only on existing APIs.

* **Browse:** a tree of every mount (gateway, Postgres, Databricks, Snowflake,
  files, Iceberg namespaces, `system`), filtered per caller with the same
  `visible`/`describe` checks as SQL and Quack.
* **Table view:** columns and types, a preview of the first 100 rows, and
  "open in SQL".
* **Permissions panel:** direct and inherited grants on the selected node,
  resolved through roles. Callers with `manage_grants` can grant and revoke
  in place, and recent audit events for that node are shown.
* **Agents panel:** each agent's AgentFS files, tool calls, session
  transcripts and long-term memories (owner/admin only).
* **Test:** `tests/catalog.mjs` in Chromium. The admin sees everything, carol
  sees only her grants, and a grant attempt by carol is refused and audited.
* **Open question:** a general catalog (all sources) or a Lakekeeper-style
  view focused on Iceberg namespaces and tables. The plan above is the general
  one.

### 4.2 Quack writes

Accept SEND_DATA (`INSERT INTO gw.…`) into AgentFS/Turso tables, so DuckDB
clients can write as well as read. This needs `write_data` checks per table.

### 4.3 Live verification of the remaining connectors

Run the existing tests against a real Azure Blob container, a Snowflake
account (ADBC), a Databricks SQL warehouse and a Lakekeeper server. This needs
credentials for each, provided through the environment.

### 4.4 Server-side embeddings

The demo agents embed text themselves before calling `/v1/memory`; with only
text, the server can do keyword search but not vector search. Adding an
`[embeddings]` config section would let the server embed `text` on upsert and
search, so any agent (Python, curl, a sandbox) gets semantic memory without
its own embedding client. It should reuse `src/demo/llm.rs`'s `Embedder`,
including the single-string handling.

### 4.5 Smaller items

* **Built-in TLS listener** for remote Quack and HTTPS without a proxy.
* **Turso Cloud sync** for AgentFS (`AgentFSOptions::with_sync`), as an
  alternative to blob push/pull for cloud sandboxes.
* **Iceberg as a transcript sink** (transcripts as an Iceberg table on Azure).
* **Row-level filtering** in permissions (row policies on top of table grants).
* **Memory hygiene:** de-duplicate near-identical memories and expire old
  `turn` memories, keeping `fact` memories.
* **Report the turso index bug upstream** (see section 3).

## 5. How to run and test

```bash
cargo run --release -- demo                         # multiplayer demo, prints a link per person
cargo test                                          # unit + integration tests (no services needed)
ENACT_DB_TEST_PG="host=… user=… password=… dbname=…" cargo test --features full
node tests/demo.mjs target/debug/enact-db           # 2 humans + 2 agents + viewer in Chromium (scripted agents)
node tests/demo_llm.mjs target/debug/enact-db       # live LLM agents (needs OPENAI_COMPATIBLE_*)
node tests/memory_live.mjs target/debug/enact-db    # live memory across a restart (also needs OPENROUTER_API_KEY)
```

Environment variables for the LLM agents:

| Variable | Purpose |
|---|---|
| `OPENAI_COMPATIBLE_BASE_URL`, `OPENAI_COMPATIBLE_API_KEY` | Turn on LLM agents |
| `OPENAI_COMPATIBLE_MODEL` | Model (default `MiniMax-M3`) |
| `OPENAI_COMPATIBLE_MAX_TOKENS` | Token budget (default 100000) |
| `OPENROUTER_API_KEY` | Turns on long-term memory in LanceDB |
| `OPENROUTER_EMBED_MODEL` | Embedding model (default `nvidia/nemotron-3.5-embed-1b:free`) |
| `OPENROUTER_BASE_URL` | Embeddings endpoint (default `https://openrouter.ai/api/v1`) |
