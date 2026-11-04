// Live LLM run: the demo with real model-driven agents (needs OPENAI_COMPATIBLE_* in the env).
// Two humans ask open-ended questions; scribe writes and sketch draws from the model's plan.
// Usage: node tests/demo_llm.mjs <path-to-enact-db> [screenshot-dir]
import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

if (!process.env.OPENAI_COMPATIBLE_BASE_URL || !process.env.OPENAI_COMPATIBLE_API_KEY) {
  console.log("SKIP: OPENAI_COMPATIBLE_BASE_URL / OPENAI_COMPATIBLE_API_KEY not set");
  process.exit(0);
}
const bin = process.argv[2] || "target/debug/enact-db";
const shots = process.argv[3];
const dir = mkdtempSync(join(tmpdir(), "enact-llm-"));
const port = 9900 + Math.floor(Math.random() * 90);
const proc = spawn(bin, ["demo", "--listen", `127.0.0.1:${port}`, "--data-dir", join(dir, "data"), "--speed-ms", "15",
  "--tokens-out", join(dir, "tokens.json")], { stdio: ["ignore", "ignore", "inherit"] });
let browser;
const cleanup = () => { try { proc.kill(); } catch (_) {} try { browser && browser.close(); } catch (_) {} };
const fail = (m) => { console.error("FAIL:", m); cleanup(); process.exit(1); };
const step = (m) => console.log("  ✓", m);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
for (let i = 0; i < 300 && !existsSync(join(dir, "tokens.json")); i++) await sleep(100);
const cfg = JSON.parse(readFileSync(join(dir, "tokens.json"), "utf8"));
const admin = { Authorization: "Bearer " + cfg.admin };

browser = await chromium.launch();
const ctx = await browser.newContext({ viewport: { width: 1440, height: 1000 }, deviceScaleFactor: shots ? 2 : 1 });
const open = async (who) => {
  const p = await ctx.newPage();
  p.on("pageerror", (e) => fail(`${who}: ${e.message}`));
  await p.goto(cfg.links[who]);
  await p.waitForFunction(() => window.demo && window.demo.synced, null, { timeout: 15000 });
  return p;
};
const [alice, bob] = [await open("alice"), await open("bob")];
await alice.waitForFunction(() => window.demo.text().includes("- Next steps"), null, { timeout: 30000 });
await bob.waitForFunction(() => window.demo.shapes().some((s) => s.author === "sketch" && s.label === "Agenda (scribe)"), null, { timeout: 30000 });
step("agents online (LLM brain), agenda written and drawn");

const textBefore = await alice.evaluate(() => window.demo.text().length);
const shapesBefore = await bob.evaluate(() => window.demo.shapes().filter((s) => s.author === "sketch").length);
const send = async (page, text) => { await page.fill("#chatin", text); await page.press("#chatin", "Enter"); };
const t0 = Date.now();
await send(alice, "@scribe draft a 3-step launch plan for our Q3 product. Owners: alice for marketing, bob for engineering. Keep it short.");
await send(bob, "@sketch draw our launch plan as a flow: Build, Beta, Launch, connected by arrows.");

await alice.waitForFunction((n) => window.demo.text().length > n + 80 && window.demo.chat().some((m) => m.author === "scribe" && !m.text.startsWith("Hi all")), textBefore, { timeout: 240000 })
  .catch(() => fail("scribe did not write"));
const wrote = await alice.evaluate((n) => window.demo.text().slice(n), textBefore);
step(`scribe (LLM) wrote ${wrote.length} chars in ${((Date.now() - t0) / 1000).toFixed(1)}s`);
await bob.waitForFunction((n) => {
  const s = window.demo.shapes().filter((x) => x.author === "sketch");
  return s.length >= n + 3 && s.some((x) => x.kind === "arrow" && x.y >= 380);
}, shapesBefore, { timeout: 240000 }).catch(() => fail("sketch did not draw"));
const drawn = await bob.evaluate(() => window.demo.shapes().filter((s) => s.author === "sketch" && s.y >= 380).map((s) => `${s.kind}:${s.label || ""}`));
step(`sketch (LLM) drew: ${drawn.join(", ")}`);
await sleep(3000); // let animations and the section-box flow settle

const [a, b] = [await alice.evaluate(() => window.demo.text()), await bob.evaluate(() => window.demo.text())];
if (a !== b) fail("replicas diverged");
const replies = await alice.evaluate(() => window.demo.chat().filter((m) => m.kind === "agent").slice(-4).map((m) => `${m.author}: ${m.text}`));
const listing = await (await fetch(`${cfg.base}/v1/agents/scribe/fs/llm`, { headers: admin })).json();
if (!listing.entries || !listing.entries.length) fail("no LLM exchange stored in scribe's AgentFS");
const rec = await (await fetch(`${cfg.base}/v1/agents/scribe/fs/llm/${listing.entries[0].name}`, { headers: admin })).json();
step(`model ${rec.model}, ${rec.latency_ms} ms, usage ${JSON.stringify(rec.usage && { prompt: rec.usage.prompt_tokens, completion: rec.usage.completion_tokens })}; stored in AgentFS /llm/${listing.entries[0].name}`);

console.log("\n--- scribe wrote ---\n" + wrote.trim() + "\n--- agent replies ---\n" + replies.join("\n"));
if (shots) {
  await alice.screenshot({ path: join(shots, "demo-llm-alice.png"), fullPage: true });
}
console.log("OK live LLM agents");
cleanup();
process.exit(0);
