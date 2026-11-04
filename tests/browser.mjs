// Browser multiplayer test: two humans (real Chromium + the stock y-websocket
// provider bundled in the binary) and one HTTP agent co-edit a document.
// Usage: node tests/browser.mjs http://127.0.0.1:9494 [screenshot.png]
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const base = process.argv[2] || "http://127.0.0.1:9494";
const shot = process.argv[3];
const doc = "pw-" + Date.now();
const fail = (m) => { console.error("FAIL:", m); process.exit(1); };

const browser = await chromium.launch();
const ctx = await browser.newContext({ viewport: { width: 1280, height: 760 }, deviceScaleFactor: 2 });
const alice = await ctx.newPage();
const bob = await ctx.newPage();
for (const p of [alice, bob]) p.on("pageerror", (e) => fail("page error: " + e.message));

await alice.goto(`${base}/?doc=${doc}&user=alice`);
await bob.goto(`${base}/?doc=${doc}&user=bob`);
for (const p of [alice, bob]) await p.waitForSelector("#status.on", { timeout: 10000 });

await alice.click("#editor");
await alice.keyboard.type("Alice drafts the plan.");
await bob.waitForFunction(() => document.querySelector("#editor").value.includes("Alice drafts the plan."), null, { timeout: 5000 });

await bob.click("#editor");
await bob.keyboard.press("End");
await bob.keyboard.type(" Bob adds a risk.");

// An agent joins over HTTP: presence + an edit.
const h = { "content-type": "application/json", "x-agent-id": "planner" };
await fetch(`${base}/v1/docs/${doc}/presence`, { method: "PUT", headers: h,
  body: JSON.stringify({ state: { user: { name: "planner", kind: "agent", color: "#7b49c8" } } }) });
await fetch(`${base}/v1/docs/${doc}/text/content`, { method: "POST", headers: h,
  body: JSON.stringify({ append: "\n[planner] Suggest a timeline." }) });

const want = ["Alice drafts the plan.", "Bob adds a risk.", "[planner] Suggest a timeline."];
for (const p of [alice, bob]) {
  await p.waitForFunction((w) => w.every((s) => document.querySelector("#editor").value.includes(s)), want, { timeout: 5000 });
}
const a = await alice.$eval("#editor", (e) => e.value);
const b = await bob.$eval("#editor", (e) => e.value);
if (a !== b) fail(`replicas diverged:\n${a}\n---\n${b}`);
const server = await (await fetch(`${base}/v1/docs/${doc}/text/content`)).text();
if (server !== a) fail("server copy differs from browsers");

await alice.waitForFunction(() => document.querySelectorAll("#peers .peer").length >= 3, null, { timeout: 5000 });
const chips = await alice.$$eval("#peers .peer", (els) => els.map((e) => e.textContent));
for (const n of ["alice", "bob", "planner"]) if (!chips.some((c) => c.includes(n))) fail("missing presence " + n + " in " + chips);

await alice.waitForFunction(() => document.querySelector("#feed").textContent.includes("doc.text.append"), null, { timeout: 8000 });
if (shot) await alice.screenshot({ path: shot });
console.log("OK browser multiplayer:", JSON.stringify(a), "presence:", chips.join(" | "));
await browser.close();
