// Multiplayer demo test: 2 humans (Chromium) + 2 agents (scribe writes, sketch draws)
// + 1 read-only viewer, all editing the same document and board concurrently.
//
// Usage: node tests/demo.mjs <path-to-enact-db-binary> [screenshot-dir]
import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const bin = process.argv[2] || "target/debug/enact-db";
const shots = process.argv[3];
const dir = mkdtempSync(join(tmpdir(), "enact-demo-"));
const port = 9600 + Math.floor(Math.random() * 300);
const fail = (m) => { console.error("FAIL:", m); cleanup(); process.exit(1); };
const step = (m) => console.log("  ✓", m);

const proc = spawn(bin, ["demo", "--listen", `127.0.0.1:${port}`, "--data-dir", join(dir, "data"),
  "--speed-ms", "12", "--tokens-out", join(dir, "tokens.json")], { stdio: ["ignore", "ignore", "inherit"] });
let browser;
function cleanup() { try { proc.kill(); } catch (_) {} try { browser && browser.close(); } catch (_) {} }
process.on("exit", cleanup);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
for (let i = 0; i < 300 && !existsSync(join(dir, "tokens.json")); i++) await sleep(100);
if (!existsSync(join(dir, "tokens.json"))) fail("demo did not start");
const cfg = JSON.parse(readFileSync(join(dir, "tokens.json"), "utf8"));
const admin = { Authorization: "Bearer " + cfg.admin };
const get = async (path, headers = admin) => (await fetch(cfg.base + path, { headers })).json();

browser = await chromium.launch();
const ctx = await browser.newContext({ viewport: { width: 1440, height: 1000 }, deviceScaleFactor: shots ? 2 : 1 });
const open = async (who) => {
  const p = await ctx.newPage();
  p.on("pageerror", (e) => fail(`${who}: ${e.message}`));
  await p.goto(cfg.links[who]);
  await p.waitForFunction(() => window.demo && window.demo.synced, null, { timeout: 15000 });
  return p;
};
const [alice, bob, carol] = [await open("alice"), await open("bob"), await open("carol")];
const waitFor = async (page, fn, arg, timeout = 20000) => {
  try { return await page.waitForFunction(fn, arg, { timeout }); }
  catch (e) {
    const st = await page.evaluate(() => ({ text: window.demo.text(), chat: window.demo.chat().map((m) => m.author + ": " + m.text),
      shapes: window.demo.shapes().map((s) => `${s.kind}/${s.author}/${s.label}`) })).catch(() => ({}));
    fail(`timeout waiting for ${fn.toString().slice(0, 160)}\nstate: ${JSON.stringify(st, null, 1)}`);
  }
};
step("3 browsers connected (alice, bob editors; carol viewer)");

// 1. presence: everyone sees 2 humans + viewer + 2 agents
for (const p of [alice, bob]) {
  await waitFor(p, () => {
    const names = window.demo.peers().map((x) => x.name);
    return ["alice", "bob", "carol", "scribe", "sketch"].every((n) => names.includes(n));
  });
}
const agentsSeen = await alice.evaluate(() => window.demo.peers().filter((p) => p.kind === "agent").map((p) => p.name).sort());
if (JSON.stringify(agentsSeen) !== '["scribe","sketch"]') fail("agents not in presence: " + agentsSeen);
step("presence: alice, bob, carol + agents scribe, sketch");

// 2. agents start working on their own
await waitFor(alice, () => window.demo.text().includes("## Agenda (scribe)\n- Goals for this session\n- Open questions\n- Next steps\n"));
await waitFor(alice, () => window.demo.shapes().some((s) => s.author === "sketch" && s.kind === "rect" && s.label === "Agenda (scribe)" && s.w === 200));
step("scribe typed the agenda; sketch drew a box for it");

// 3. humans write and draw concurrently with the agents
const typeAt = async (page, where, text) => {
  await page.click("#editor");
  await page.keyboard.press(where === "start" ? "Control+Home" : "Control+End");
  await page.keyboard.type(text, { delay: 15 });
};
const draw = async (page, kind, pts) => {
  await page.click(`[data-tool="${kind}"]`);
  await page.locator("#board").scrollIntoViewIfNeeded();
  const box = await page.locator("#board").boundingBox();
  const at = ([x, y]) => [box.x + (x / 1000) * box.width, box.y + (y / 700) * box.height];
  await page.mouse.move(...at(pts[0]));
  await page.mouse.down();
  for (const p of pts.slice(1)) await page.mouse.move(...at(p), { steps: 4 });
  await page.mouse.up();
};
// Each person acts sequentially (one keyboard/mouse each); the people, and the agents, run concurrently.
await Promise.all([
  (async () => {
    await alice.fill("#chatin", "@scribe summarize"); // scribe starts typing while alice writes below
    await alice.press("#chatin", "Enter");
    await typeAt(alice, "end", "\n## Budget\nAlice: budget is 10k\n");
  })(),
  (async () => {
    await typeAt(bob, "start", "# Q3 launch plan\nBob: launch in May\n");
    await draw(bob, "pen", [[120, 560], [180, 600], [240, 560], [300, 610], [360, 570]]);
  })(),
]);
await draw(alice, "rect", [[420, 520], [600, 640]]);
await bob.fill("#chatin", "@sketch box Risks");
await bob.press("#chatin", "Enter");
await bob.fill("#chatin", "@sketch smile");
await bob.press("#chatin", "Enter");
step("alice + bob typed, drew and messaged while agents worked");

// 4. agents respond to chat
await waitFor(alice, () => window.demo.text().includes("## Summary (scribe)") && /Contributors: .*alice.*bob/.test(window.demo.text()), null, 30000);
const summary = await alice.evaluate(() => window.demo.text().split("## Summary (scribe)")[1]);
if (!/- [1-9]\d* sections?: .*Agenda \(scribe\)/.test(summary)) fail("summary missed the document's sections: " + JSON.stringify(summary));
await waitFor(bob, () => window.demo.shapes().some((s) => s.author === "sketch" && s.label === "Risks"), null, 30000);
await waitFor(bob, () => window.demo.shapes().filter((s) => s.author === "sketch" && s.kind === "pen").length >= 4, null, 30000);
await waitFor(alice, () => window.demo.shapes().some((s) => s.author === "sketch" && s.label === "Budget"), null, 30000);
const replies = await alice.evaluate(() => window.demo.chat().filter((m) => m.kind === "agent").map((m) => m.author + ": " + m.text));
if (!replies.some((r) => r.startsWith("scribe: Summary added")) || !replies.some((r) => r.includes('box "Risks" for bob')))
  fail("agent replies missing: " + JSON.stringify(replies));
step("scribe summarized (listing contributors); sketch drew Risks, a smile, and a box for the new Budget section");

// 5. convergence: all replicas identical, server included
await sleep(1500);
const snap = (p) => p.evaluate(() => ({ text: window.demo.text(), shapes: JSON.stringify(window.demo.shapes()), chat: window.demo.chat().length }));
const [a, b, c] = [await snap(alice), await snap(bob), await snap(carol)];
const serverText = await (await fetch(`${cfg.base}/v1/docs/demo/text/doc`, { headers: admin })).text();
if (a.text !== b.text || b.text !== c.text || a.text !== serverText) fail("text diverged");
if (a.shapes !== b.shapes || b.shapes !== c.shapes) fail("board diverged");
if (a.chat !== b.chat || b.chat !== c.chat) fail("chat diverged");
for (const part of ["# Q3 launch plan\nBob: launch in May\n", "## Budget\nAlice: budget is 10k\n", "## Agenda (scribe)", "## Summary (scribe)"])
  if (!a.text.includes(part)) fail("missing in doc: " + part);
const authors = [...new Set(JSON.parse(a.shapes).map((s) => s.author))].sort();
if (JSON.stringify(authors) !== '["alice","bob","sketch"]') fail("shape authors: " + authors);
step(`converged: ${a.text.length} chars, ${JSON.parse(a.shapes).length} shapes, ${a.chat} messages identical in all 3 browsers and on the server`);

// 6. carol is read-only: UI disabled, edits rejected server-side
const carolUi = await carol.evaluate(() => ({ role: document.getElementById("role").textContent, ro: document.getElementById("editor").readOnly,
  pen: document.querySelector('[data-tool="pen"]').disabled, chat: document.getElementById("chatin").disabled }));
if (carolUi.role !== "read-only" || !carolUi.ro || !carolUi.pen || !carolUi.chat) fail("carol UI not read-only: " + JSON.stringify(carolUi));
await carol.click("#editor");
await carol.keyboard.type("carol was here");
const carolApi = await fetch(`${cfg.base}/v1/docs/demo/text/doc`, { method: "POST", headers: { ...admin, Authorization: "Bearer " + cfg.tokens.carol, "content-type": "application/json" },
  body: JSON.stringify({ append: "carol was here" }) });
if (carolApi.status !== 403) fail("carol API write should be 403, got " + carolApi.status);
await sleep(500);
if ((await alice.evaluate(() => window.demo.text())).includes("carol was here")) fail("viewer edit leaked");
step("carol (viewer) watches live but cannot edit: UI disabled, API 403");

// 7. undo is per user: alice's undo removes only alice's changes
const before = await alice.evaluate(() => window.demo.shapes().length);
await alice.click("#undo"); // her rect
await waitFor(alice, (n) => window.demo.shapes().length === n - 1 && !window.demo.shapes().some((s) => s.author === "alice"), before);
await alice.click("#undo"); // her typing
await waitFor(bob, () => !window.demo.text().includes("Alice: budget is 10k"));
const after = await bob.evaluate(() => window.demo.text());
if (!after.includes("Bob: launch in May") || !after.includes("## Agenda (scribe)")) fail("undo touched other people's work");
if (!(await bob.evaluate(() => window.demo.shapes().some((s) => s.author === "bob" && s.kind === "pen")))) fail("bob's stroke lost");
step("alice's Undo removed only her own box and text; bob's and the agents' work stayed");

// 8. everything is recorded: transcripts, agent tool calls, audit log, activity feed
const tr = await get("/v1/transcripts/demo");
const roles = new Set(tr.entries.map((e) => `${e.role}:${e.actor}`));
for (const r of ["user:human:alice", "user:human:bob", "assistant:agent:scribe", "assistant:agent:sketch"])
  if (![...roles].includes(r)) fail("transcript missing " + r + " in " + [...roles]);
const scribeTools = (await get("/v1/agents/scribe/tools")).tool_calls.map((t) => t.name);
const sketchTools = (await get("/v1/agents/sketch/tools")).tool_calls.map((t) => t.parameters && t.parameters.label);
if (!scribeTools.includes("doc.summarize") || !sketchTools.includes("Risks")) fail("tool calls missing");
const audit = await get("/v1/authz/audit?principal=user:carol&decision=deny&limit=20");
if (!audit.events.some((e) => e.action === "write_data" && e.resource === "doc:demo")) fail("carol's denied write not audited");
await waitFor(alice, () => [...document.querySelectorAll("#feed li")].some((li) => li.textContent.includes("doc.summarize")), null, 10000);
step("transcripts (humans + agents), agent tool calls, audit log and live activity feed all recorded");

if (shots) {
  await alice.bringToFront();
  await alice.evaluate(() => window.scrollTo(0, 0));
  await alice.screenshot({ path: join(shots, "demo-alice.png"), fullPage: true });
  await bob.screenshot({ path: join(shots, "demo-bob.png"), fullPage: true });
  await carol.screenshot({ path: join(shots, "demo-carol.png"), fullPage: true });
}
console.log("OK multiplayer demo: 2 humans + 2 agents + 1 viewer");
cleanup();
process.exit(0);
