# agentgate

One Rust binary for **multiplayer AI workspaces**: humans in browsers and AI
agents (local or in cloud sandboxes) working on the same live state, with
memory, transcripts, and enterprise data behind a single endpoint.

```
 Browser (Yjs + y-websocket)        Agents (HTTP / Yjs / pycrdt)       Cloud sandbox agents
          │  ws://…/ws/{doc}                 │  /v1/...                  agentgate --pull-agent X
          └──────────────────┬───────────────┘                                   │
                             ▼                                                   │
┌───────────────────────────── agentgate (single binary) ─────────────────────────┴───┐
│  CRDT hub (yrs)       AgentFS sandboxes      Memory           Transcripts           │
│  y-protocols sync,    per-agent Turso file:  turso vectors |  local outbox ──► DuckDB│
│  presence, fan-out,   fs + kv + tool_calls   pgvector |       (cursor per sink) ──► │
│  persisted log        (agentfs-compatible)   LanceDB                       Postgres │
│                                                                                     │
│  DataFusion federation: Postgres · Snowflake (ADBC) · Azure Blob Parquet/CSV/JSON · │
│  local files · gateway.system.* live tables (audit, kv, docs, memory, transcripts)  │
│                                                                                     │
│  Blob tier (object_store: Azure / file://): agent sandbox push/pull + artifacts     │
└─────────────────────────────────────────────────────────────────────────────────────┘
        local:  data/hub.db (Turso) · data/agents/*.db (AgentFS) · DuckDB · LanceDB
        cloud:  Postgres (memory, transcripts) · Azure Blob · Snowflake/Databricks
```

## Why this shape

* **No file-lock fights.** Only the gateway opens SQLite/Turso files. Agents
  and humans talk to it over HTTP/WebSocket, so concurrency is handled in one
  process: CRDT merges for shared documents, a single serialized writer for the
  hub DB, and one AgentFS file per agent (agents never contend with each other).
* **Real AgentFS, not a look-alike.** Sandboxes use `agentfs-sdk`, so
  `agentfs fs ls data/agents/researcher.db` and `agentfs timeline` work on them.
* **Real Yjs protocol.** Stock `y-websocket` in the browser connects unchanged;
  agents can use Yjs, `yrs` or `pycrdt` over the same socket, or plain HTTP ops.
* **Local first, cloud when configured.** With no config, everything runs on
  local Turso. Postgres, Azure, DuckDB, LanceDB and Snowflake turn on per section.

## Build

```bash
cargo build --release                                  # azure + postgres (default)
cargo build --release --features duckdb                # + DuckDB transcript sink
cargo build --release --features lancedb               # + LanceDB memory backend
cargo build --release --features full                  # everything except snowflake
cargo build --release --features snowflake             # + Snowflake via ADBC driver
```

The result is `target/release/agentgate`. The Snowflake feature loads the ADBC
Snowflake driver shared library at runtime. Everything else is compiled in,
including DuckDB's `json` extension and the browser's Yjs/y-websocket bundle,
so the binary never downloads anything at runtime and works air-gapped.

Build prerequisites: a Rust toolchain, a C/C++ compiler (DuckDB), `protoc`
for `lancedb` (`apt-get install protobuf-compiler`), and OpenSSL headers (Postgres TLS).

To change the browser bundle (`src/web/yjs-bundle.js`), run
`cd web && npm install && npm run build`.

Postgres connection strings follow libpq `sslmode` semantics: `prefer` and
`require` encrypt without verifying the certificate, `verify-ca` checks the
chain, and `verify-full` also checks the hostname. Use `verify-full` for
managed cloud Postgres.

## Run

```bash
./agentgate                                   # local only, 127.0.0.1:9494, ./data
./agentgate --config agentgate.toml
AGENTGATE_TOKEN=secret ./agentgate --listen 0.0.0.0:9494
./agentgate --config cloud.toml --pull-agent researcher   # cloud sandbox boot
```

Open `http://127.0.0.1:9494/` for the built-in multiplayer workspace: a Yjs
editor with presence and a live agent-activity feed. Open it in two tabs and
use "Ask an agent to write" to see a human and an agent co-editing. With auth
enabled, add `?token=…` to the URL.

See [`agentgate.example.toml`](agentgate.example.toml) for every option.

## API

Callers identify themselves with `X-Agent-Id: <id>` (agents) or `X-Actor: <name>`
(humans). With `auth_token` set, send `Authorization: Bearer <token>`. The token
can also go in `?token=` for WebSockets, since browsers can't set WebSocket headers.

### Multiplayer documents (CRDT)

| | |
|---|---|
| `GET /ws/{doc}?user=alice` / `?agent=writer` | y-websocket endpoint (`new WebsocketProvider('ws://host/ws', doc, ydoc)`) |
| `GET /v1/docs` | persisted + live documents |
| `GET /v1/docs/{doc}` | JSON snapshot of all root types + presence |
| `GET/POST /v1/docs/{doc}/text/{name}` | read text; `{"append": s}`, `{"insert": {"index", "text"}}`, `{"delete": {"index", "len"}}`, `{"replace": s}` |
| `GET/PATCH /v1/docs/{doc}/map/{name}` | read map; patch keys (`null` deletes) |
| `GET/POST /v1/docs/{doc}/array/{name}` | read array; `{"push": value}` |
| `GET /v1/docs/{doc}/update[?sv=b64]` | binary Yjs update (full or diff) |
| `POST /v1/docs/{doc}/update` | apply a binary Yjs v1 update produced by an agent's own doc |
| `PUT /v1/docs/{doc}/presence` | show an HTTP agent in everyone's presence list |

Every edit is broadcast to all peers and appended to `hub.db`. The log is
compacted into snapshots every `compact_after` updates, and documents reload on restart.

### Agent sandboxes (AgentFS)

| | |
|---|---|
| `GET /v1/agents` | agents on disk / open |
| `GET/PUT/DELETE /v1/agents/{id}/fs/{path}` | files (GET on a directory lists it) |
| `POST /v1/agents/{id}/mkdir/{path}` | `mkdir -p` |
| `GET/PUT/DELETE /v1/agents/{id}/kv/{key}` | JSON key-value |
| `GET/POST /v1/agents/{id}/tools` | tool-call audit trail (writes are auto-audited) |
| `POST /v1/agents/{id}/push` · `/pull` | sync the sandbox file with the blob tier |

### Memory

| | |
|---|---|
| `POST /v1/memory` | upsert one or many `{namespace, id, content, metadata?, embedding?}` |
| `POST /v1/memory/search` | `{namespace, embedding? \| text?, k}` → nearest by cosine distance |
| `DELETE /v1/memory/{ns}/{id}` | delete |

Backends: `turso` (local, default), `postgres` (pgvector), and `lancedb` (local
path or `az://`). Embeddings are supplied by the caller.

### Transcripts

| | |
|---|---|
| `POST /v1/transcripts` | append one or many `{session_id, role, content, actor?, metadata?}` |
| `GET /v1/transcripts/{session}?after=` | read back |

Entries land in the local outbox first, then export to every sink (DuckDB,
Postgres). Each sink has its own cursor, retries with backoff, and inserts
idempotently, so a laptop that's offline from Postgres catches up later.

### SQL

`POST /v1/query {"sql": "...", "format": "json" | "arrow"}` runs over every
configured source plus the live system tables:

* `system.agent_tool_calls` (agent_id, name, parameters, result, status, timings)
* `system.agent_kv`
* `system.crdt_docs`
* `system.local_memories`
* `system.transcripts`

```sql
SELECT c.name, count(*) AS actions
FROM customers c JOIN system.agent_tool_calls t ON t.agent_id = c.owner_agent
GROUP BY c.name;
```

Queries are read-only unless `allow_sql_writes = true`. Agent queries (with
`X-Agent-Id`) are audited.

### Blob artifacts

`PUT/GET /v1/blob/{path}` and `GET /v1/blob` store files under
`<blob url>/artifacts/…`, so sandboxed agents never hold cloud credentials.

## Tests

```bash
cargo test                                   # unit + tests/e2e.rs (no services needed)
AGENTGATE_TEST_PG="host=127.0.0.1 user=agent password=agentpw dbname=agents" \
  cargo test --features full                 # + real Postgres/pgvector, DuckDB, LanceDB
node tests/browser.mjs http://127.0.0.1:9494 # two Chromium users + an HTTP agent co-editing
```

* `tests/e2e.rs` boots the binary and covers auth, AgentFS files, KV and audit,
  SQL federation (CSV ⨝ live audit table), two y-websocket peers plus an HTTP
  agent converging, presence, Turso vector memory, transcripts, blob artifacts,
  restart persistence, CRDT log compaction, and sandbox push → wipe → pull.
* `tests/backends.rs` covers a Postgres source joined with agent activity,
  pgvector memory, transcript export to Postgres and DuckDB, and LanceDB memory.
* `tests/browser.mjs` runs the built-in page in Chromium: two humans type and
  an agent edits over HTTP, then it checks that all copies match, that presence
  shows `alice | bob | planner (agent)`, and that the SQL-driven activity feed updates.

## Status and roadmap

Verified as above: CRDT hub, AgentFS sandboxes, all memory backends (Turso,
pgvector, LanceDB), the transcript outbox with DuckDB and Postgres sinks, SQL
federation (files, Postgres, system tables), and the blob tier (`file://`).

Implemented but not yet verified against the live service: Azure Blob (it goes
through the same `object_store` code path as `file://`, plus `az://`
credentials) and Snowflake via ADBC.

Known upstream issue: in turso 0.4.4, an index on `(x, rowid_alias)` makes
`x = ? AND rowid_alias > ?` return no rows. `src/store.rs` works around it with
`+seq`; it's worth reporting upstream.

Next:
* **Quack (DuckDB wire protocol) endpoint.** Quack is DuckDB's experimental
  client-server protocol, introduced May 2026. Its wire format is DuckDB's internal binary
  serializer plus `DataChunk`s over `POST /quack`, with no published spec, so it
  needs a Rust port of that serialization. Until then, DuckDB clients can query
  with `read_json`/Arrow over HTTP.
* **Databricks.** Add an ODBC source (`datafusion-table-providers/odbc`) or
  the Databricks SQL Statement API as a TableProvider.
* **Server-side embeddings** (call an embedding model on upsert/search), **per-doc ACLs**, and
  **Turso Cloud sync** for AgentFS (`AgentFSOptions::with_sync`) as an alternative to blob push/pull.
