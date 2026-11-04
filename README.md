# enact-db

One Rust binary for **multiplayer AI workspaces**: humans in browsers and AI
agents (local or in cloud sandboxes) working on the same live state, with
memory, transcripts, and enterprise data behind a single endpoint.

```
 Browser (Yjs + y-websocket)        Agents (HTTP / Yjs / pycrdt)       Cloud sandbox agents
          │  ws://…/ws/{doc}                 │  /v1/...                  enact-db --pull-agent X
          └──────────────────┬───────────────┘                                   │
                             ▼                                                   │
┌───────────────────────────── enact-db (single binary) ─────────────────────────┴───┐
│  CRDT hub (yrs)       AgentFS sandboxes      Memory           Transcripts           │
│  y-protocols sync,    per-agent Turso file:  turso vectors |  local outbox ──► DuckDB│
│  presence, fan-out,   fs + kv + tool_calls   pgvector |       (cursor per sink) ──► │
│  persisted log        (agentfs-compatible)   LanceDB                       Postgres │
│                                                                                     │
│  DataFusion federation: Postgres · Databricks · Snowflake (ADBC) · Iceberg REST     │
│  (Lakekeeper/Polaris/Unity) · Azure Blob/S3 files · system.* live tables · Quack    │
│                                                                                     │
│  Permissions (Lakekeeper model): users/agents/roles, grants on a resource tree,     │
│  OIDC + API tokens, admission gates, ownership-on-create, versioned audit log       │
│                                                                                     │
│  Blob tier (object_store: Azure / file://): agent sandbox push/pull + artifacts     │
└─────────────────────────────────────────────────────────────────────────────────────┘
        local:  data/hub.db (Turso) · data/agents/*.db (AgentFS) · DuckDB · LanceDB
        cloud:  Postgres (memory, transcripts) · Azure Blob · Iceberg · Snowflake/Databricks
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
cargo build --release                                  # azure + postgres + iceberg (default)
cargo build --release --features duckdb                # + DuckDB transcript sink
cargo build --release --features lancedb               # + LanceDB memory backend
cargo build --release --features full                  # everything except snowflake
cargo build --release --features snowflake             # + Snowflake via ADBC driver
```

The result is `target/release/enact-db`. The Snowflake feature loads the ADBC
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
./enact-db                                   # local only, 127.0.0.1:9494, ./data
./enact-db --config enact-db.toml
ENACT_DB_TOKEN=secret ./enact-db --listen 0.0.0.0:9494   # non-loopback requires auth
./enact-db --config cloud.toml --pull-agent researcher   # cloud sandbox boot
```

Open `http://127.0.0.1:9494/` for the built-in multiplayer workspace: a Yjs
editor with presence and a live agent-activity feed. Open it in two tabs and
use "Ask an agent to write" to see a human and an agent co-editing. With auth
enabled, add `?token=…` to the URL.

Without an `[auth]` section the server runs in **open mode** (every request
allowed, logged as a warning). It refuses to start in open mode on anything but
a loopback address.

See [`enact-db.example.toml`](enact-db.example.toml) for every option.

## API

Every request carries a credential: `Authorization: Bearer <token>` (an API
token, an OIDC JWT, or the break-glass admin token), or `?token=` on
WebSockets, since browsers can't set WebSocket headers. `X-Agent-Id: <id>` acts as
that agent, which needs `assume` on `agent:<id>` unless the token *is* that agent.
Each endpoint checks the privilege it needs (see [Permissions](#permissions)).

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

### Quack (DuckDB client-server protocol)

enact-db speaks Quack v3 at `POST /quack` on its main port, so any DuckDB can
attach the whole gateway (federated sources plus live agent state) as a database:

```sql
CREATE SECRET (TYPE quack, TOKEN 'edb_…');          -- any enact-db credential
ATTACH 'quack:localhost:9494' AS gw;
FROM gw.customers;                                 -- Postgres / Databricks / Snowflake / files
SELECT agent_id, count(*) FROM gw.system.agent_tool_calls GROUP BY ALL;
FROM quack_query('gw', 'SELECT ... any DataFusion SQL ...');
```

* The secret's token is authenticated like any API call. The catalog the client
  sees lists only tables that principal may see, and every scan or
  `quack_query` is checked per table.
* At ATTACH, the client's catalog queries (`duckdb_schemas()`/`duckdb_tables()`)
  are answered from enact-db's catalog: `public` appears as `main`, next to `system`.
* Scans arrive as `SELECT #1, #3 FROM main.t` and run on DataFusion. Results
  stream as DuckDB DataChunks: rows are inlined in PREPARE up to `inline_rows`,
  then sent as FETCH batches. Retries and out-of-order fetch-ahead are served
  from a cache until the client acknowledges them.
* Read-only: SEND_DATA, INSERT and DDL are refused with a `Permission`/read-only error.
* Local hosts connect over plain HTTP. For remote clients, put TLS in front
  (DuckDB defaults to HTTPS for non-local hosts), or have the client attach with SSL disabled.
* Types: booleans, all integer widths, FLOAT/DOUBLE, DECIMAL(≤38), VARCHAR,
  BLOB, DATE, TIME, TIMESTAMP[TZ], INTERVAL, LIST and STRUCT are sent natively.
  Anything else is sent as VARCHAR.

### Databricks

A `kind = "databricks"` source calls the Databricks SQL Statement Execution API
directly, so no ODBC/JDBC driver is needed. Results come back as Arrow over
`EXTERNAL_LINKS` and stream chunk by chunk. Projections, `LIMIT` and simple
filters are pushed down to the warehouse (filters are re-checked locally). Auth
is a PAT/Entra token or OAuth M2M with a service principal. The Databricks token
is never sent to the presigned storage links; only the headers Databricks returns are.

### Apache Iceberg (Lakekeeper, Polaris, Unity Catalog, …)

Each `[[iceberg]]` entry mounts an Iceberg REST catalog as a DataFusion catalog:

```toml
[[iceberg]]
name = "lake"                                  # -> lake.<namespace>.<table>
uri = "http://lakekeeper:8181/catalog"
warehouse = "prod"
credential_env = "LAKE_CLIENT_CREDENTIALS"     # "client_id:secret" (OAuth2), or token_env = "…"
scope = "lakekeeper"
```

```sql
SELECT c.name, sum(o.amount) FROM lake.sales.orders o JOIN customers c ON c.id = o.customer_id GROUP BY 1;
```

* Uses the `iceberg` and `iceberg-catalog-rest` crates. Storage goes through
  enact-db's own `object_store` bridge (local, S3, ADLS/Blob), and per-table
  credentials vended by the catalog are honoured.
* Every query reloads table metadata, so a new snapshot is visible immediately.
  Projections and typed filters are pushed down (partition and file pruning),
  and DataFusion re-checks the filters.
* Nested namespaces appear as `a.b`. Over Quack the catalog shows up as nested
  schemas (`gw.lake.sales.orders`). Tables are permission-checked as
  `table:lake/<namespace>/<table>`.

### Blob artifacts

`PUT/GET /v1/blob/{path}` and `GET /v1/blob` store files under
`<blob url>/artifacts/…`, so sandboxed agents never hold cloud credentials.

## Permissions

The model follows Lakekeeper's (Apache-2.0) OpenFGA authorization model; the
implementation is enact-db's own (`src/authz/`).

**Principals:** `user:<id>`, `agent:<id>`, `role:<id>`. Roles contain users,
agents or other roles, transitively.

**Resources** form a tree. A grant applies to the node and everything below it:

```text
server ─┬─ catalog:<c> ── schema:<c>/<s> ── table:<c>/<s>/<t>      (gateway, iceberg, …)
        ├─ docs ── doc:<id>                ├─ agents ── agent:<id>
        ├─ memories ── memory:<ns>         ├─ transcripts ── transcript:<session>
        ├─ blob ── blob:<dir> ── blob:<dir>/<file>
        └─ roles ── role:<id>
```

**Privileges:**
* `ownership` implies everything.
* `modify ⇒ write_data ⇒ select ⇒ describe`, and `create ⇒ describe`.
* `manage_grants` lets you grant and revoke anything on the subtree.
* `pass_grants` lets you re-grant privileges you already hold (but not ownership or grant rights).
* `assume` on `agent:<id>` lets a human or service act as that agent. Audit
  events record both identities.

**What each operation needs:**

| Operation | Needs |
|---|---|
| SQL / Quack query | `select` on every table the plan touches (subqueries included); `information_schema` needs `describe` on the catalog; DDL/DML need `ownership` on `server` |
| Read / edit a doc, WebSocket | `select` / `write_data` on `doc:<id>`. Peers without `write_data` connect read-only, and their updates are dropped |
| Agent sandbox fs/kv/tools | `select` / `write_data` on `agent:<id>` (an agent always owns its own sandbox); push/pull needs `modify` |
| Memory, transcripts, blob | `select` / `write_data` on `memory:<ns>`, `transcript:<session>`, `blob:<path>` |
| Create a doc / memory namespace / transcript session / sandbox | `create` on the container. The creator becomes the owner |
| Listings (`/v1/info`, docs, agents, Quack catalog) | show only what you can `describe` or hold a grant below |

**Authentication:**
* **API tokens** (`edb_…`, stored as SHA-256 hashes, revocable).
* **OIDC JWTs** (Entra ID, Okta, Keycloak…), validated via JWKS. **Admission gates**
  (`required_claims`) reject tokens and name the failing claim. Values in the
  roles claim become provider-managed `role:oidc:<name>` roles, which can't be edited locally.
* A **break-glass** `admin_token`, which bypasses checks but is always audited.

```toml
[auth]
admin_token = "…"                                # break-glass (or ENACT_DB_TOKEN)
[auth.oidc]
issuer = "https://login.microsoftonline.com/<tenant>/v2.0"
audience = ["api://enact-db"]
agent_claim = { claim = "idtyp", value = "app" }    # app-only tokens are agents
required_claims = { tid = "<tenant-id>" }
[[auth.roles]]
id = "analysts"
members = ["user:alice"]
[[auth.grants]]
principal = "role:oidc:member"
resource = "docs"
privilege = "create"
```

**API** (`/v1/authz/…`):
* `whoami`
* `check`: batch check, reporting which grant determined each result
* `grants`: list, also as a subtree; grant; revoke
* `roles` and `roles/{id}/members`
* `tokens`: issue for yourself, for an agent you may `assume`, or for anyone as an admin; list; revoke
* `audit`

Grants and the audit log are also SQL tables: `system.grants` and `system.audit_log`.

**Audit log** (`version = 1`): one entry per decision (allow and deny) and per
permission change. Each entry records the principal, `acting_for`, source
(http/ws/quack), action, resource, decision, `determined_by` (the exact grant),
reason, user agent and detail.

## Tests

```bash
cargo test                                   # unit + tests/e2e.rs (no services needed)
ENACT_DB_TEST_PG="host=127.0.0.1 user=agent password=agentpw dbname=agents" \
  cargo test --features full                 # + real Postgres/pgvector, DuckDB, LanceDB
node tests/browser.mjs http://127.0.0.1:9494 # two Chromium users + an HTTP agent co-editing
```

* `tests/e2e.rs` boots the binary and covers auth, AgentFS files, KV and audit,
  SQL federation (CSV ⨝ live audit table), two y-websocket peers plus an HTTP
  agent converging, presence, Turso vector memory, transcripts, blob artifacts,
  restart persistence, CRDT log compaction, and sandbox push → wipe → pull.
* `tests/backends.rs` covers a Postgres source joined with agent activity,
  pgvector memory, transcript export to Postgres and DuckDB, and LanceDB memory.
* `tests/quack.rs` is a Quack v3 conformance test with its own decoder of DuckDB's
  binary format, written from the C++ sources. It covers the exact ATTACH catalog
  queries, projected scans, nulls, decimals, lists, 100k rows across PREPARE and
  FETCH batches (fetch-ahead, retries, terminal total), errors, read-only
  enforcement and disconnect.
* `tests/databricks.rs` runs against a mock of the Statement Execution API
  (polling, multi-chunk Arrow, pushdown SQL, token isolation, error reporting).
* `tests/authz.rs` acts out a multiplayer permissions scenario: OIDC admission
  gate, provider roles, API tokens, schema-level grants via roles, subquery
  smuggling blocked, doc ownership on create, sharing, a read-only WebSocket
  peer whose edits are dropped, `pass_grants` limits, `assume` with
  dual-identity audit, batch check with `determined_by`, filtered listings,
  token revocation, the Quack catalog filtered per principal, and the audit log over SQL.
* `tests/iceberg.rs` writes a real Iceberg table with iceberg-rust and serves it
  through a spec-shaped REST catalog with a bearer token. It covers pushdown,
  `count(*)`, a join with a CSV source, a new snapshot appearing mid-test, and Quack.
* `tests/browser.mjs` runs the built-in page in Chromium: two humans type and
  an agent edits over HTTP, then it checks that all copies match, that presence
  shows `alice | bob | planner (agent)`, and that the SQL-driven activity feed updates.

## Status and roadmap

Verified as above: CRDT hub, AgentFS sandboxes, all memory backends (Turso,
pgvector, LanceDB), the transcript outbox with DuckDB and Postgres sinks, SQL
federation (files, Postgres, system tables), and the blob tier (`file://`).

Implemented but not yet verified against the live service: Azure Blob (it goes
through the same `object_store` code path as `file://`, plus `az://`
credentials), Snowflake via ADBC, a live Databricks workspace (tested against
an API-contract mock), and a live DuckDB client over Quack (tested against an
independent decoder of DuckDB's wire format).

Known upstream issue: in turso 0.4.4, an index on `(x, rowid_alias)` makes
`x = ? AND rowid_alias > ?` return no rows. `src/store.rs` works around it with
`+seq`; it's worth reporting upstream.

Next:
* **Quack writes**: accept SEND_DATA (`INSERT INTO gw...`) into AgentFS/Turso tables.
* **TLS listener** built in, for remote Quack and HTTPS without a proxy.
* **Server-side embeddings** (call an embedding model on upsert/search), **per-doc ACLs**, and
  **Turso Cloud sync** for AgentFS (`AgentFSOptions::with_sync`) as an alternative to blob push/pull.
