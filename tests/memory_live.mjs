// Live agent memory across a full restart: MiniMax (or any OpenAI-compatible model)
// + OpenRouter embeddings + LanceDB. Needs OPENAI_COMPATIBLE_* and OPENROUTER_API_KEY.
// Usage: node tests/memory_live.mjs <path-to-enact-db>
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

if (!process.env.OPENAI_COMPATIBLE_API_KEY || !process.env.OPENROUTER_API_KEY) { console.log("SKIP: keys not set"); process.exit(0); }
const bin = process.argv[2] || "target/debug/enact-db";
const dir = mkdtempSync(join(tmpdir(), "enact-mem-"));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let proc;
const fail = (m) => { console.error("FAIL:", m); try { proc.kill(); } catch (_) {} process.exit(1); };
const step = (m) => console.log("  ✓", m);

async function start(doc) {
  const tok = join(dir, "tokens.json");
  if (existsSync(tok)) rmSync(tok);
  const port = 9800 + Math.floor(Math.random() * 90);
  proc = spawn(bin, ["demo", "--listen", `127.0.0.1:${port}`, "--data-dir", join(dir, "data"), "--speed-ms", "5", "--doc", doc, "--tokens-out", tok],
    { stdio: ["ignore", "pipe", "inherit"] });
  let banner = ""; proc.stdout.on("data", (d) => { banner += d; });
  for (let i = 0; i < 600 && !existsSync(tok); i++) await sleep(100);
  if (!existsSync(tok)) fail("demo did not start");
  await sleep(300);
  const cfg = JSON.parse(readFileSync(tok, "utf8"));
  cfg.memoryLine = (banner.match(/Agent memory: .*/) || [""])[0];
  return cfg;
}
const api = (cfg, token, method, path, body) => fetch(cfg.base + path, { method, headers: { Authorization: "Bearer " + token, "content-type": "application/json" },
  body: body ? JSON.stringify(body) : undefined }).then((r) => r.json());
async function ask(cfg, text) {
  const before = (await api(cfg, cfg.admin, "GET", `/v1/docs/${cfg.doc}/array/chat`)).filter((m) => m.author === "scribe").length;
  await api(cfg, cfg.tokens.alice, "POST", `/v1/docs/${cfg.doc}/array/chat`, { push: { id: String(Date.now()), author: "alice", kind: "human", color: "#2f6fde", text, ts: Date.now() } });
  for (let i = 0; i < 1200; i++) {
    const chat = (await api(cfg, cfg.admin, "GET", `/v1/docs/${cfg.doc}/array/chat`)).filter((m) => m.author === "scribe");
    if (chat.length > before && !chat[chat.length - 1].text.startsWith("Hi all")) return chat[chat.length - 1].text;
    await sleep(250);
  }
  fail("no reply to: " + text);
}

// --- session 1: decide something and ask scribe to remember it
let cfg = await start("planning");
if (!cfg.memoryLine.includes("LanceDB")) fail("memory not on LanceDB: " + cfg.memoryLine);
step(cfg.memoryLine.trim());
await sleep(2500);
let r = await ask(cfg, "@scribe please remember this decision: the Atlas launch budget is 40k EUR, approved by bob on Monday.");
step(`scribe: ${JSON.stringify(r)}`);
await sleep(4000); // let it store the fact and the turn
const hits = await api(cfg, cfg.tokens.scribe, "POST", "/v1/memory/search", { namespace: "agent-scribe", text: "Atlas", k: 10 });
if (!hits.hits || !hits.hits.some((h) => /40k/i.test(h.content))) fail("fact not in LanceDB: " + JSON.stringify(hits));
step(`LanceDB holds ${hits.hits.length} memories for scribe, incl. ${JSON.stringify(hits.hits.find((h) => /40k/i.test(h.content)).content.slice(0, 90))}`);
proc.kill(); await sleep(1500);

// --- session 2: brand-new process, same data dir, and a *different* document:
// no shared chat and no session history, so the answer can only come from LanceDB.
cfg = await start("standup");
await sleep(2500);
r = await ask(cfg, "@scribe quick question: what budget did we approve for Atlas, and who approved it?");
step(`after restart, scribe: ${JSON.stringify(r)}`);
if (!/40\s?k|40,?000/i.test(r) || !/bob/i.test(r)) fail("did not recall the decision after restart");
// the agent writes its /llm record after replying and storing memories: poll for it
let rec;
for (let i = 0; i < 120 && !rec; i++) {
  const files = await api(cfg, cfg.admin, "GET", "/v1/agents/scribe/fs/llm");
  const recs = await Promise.all((files.entries || []).map((e) => api(cfg, cfg.admin, "GET", `/v1/agents/scribe/fs/llm/${e.name}`)));
  rec = recs.filter((x) => /what budget/.test(x.request || "")).sort((x, y) => x.ts - y.ts).pop();
  if (!rec) await sleep(500);
}
if (!rec) fail("no LLM record for the follow-up question");
if (rec.history_turns !== 0) fail("expected a fresh session on the new doc, got history turns: " + rec.history_turns);
const outsideMemory = rec.memories.reduce((c, m) => c.split(m).join(""), rec.context);
if (/40\s?k/i.test(outsideMemory)) fail("fact reached the prompt outside memory recall");
if (!rec.memories.some((m) => /40k/i.test(m))) fail("LanceDB recall did not return the fact: " + JSON.stringify(rec.memories));
step(`answer came from LanceDB recall (${rec.memories.length} memories in the prompt, 0 session turns, different doc): ${JSON.stringify(rec.memories.find((m) => /40k/i.test(m)).slice(0, 100))}`);
console.log("OK live memory across restart");
proc.kill(); process.exit(0);
