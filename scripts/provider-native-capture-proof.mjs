#!/usr/bin/env node
// Compiled capture code with recorded/synthetic provider responses; no account or Chrome attach.
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import vm from "node:vm";
import { webcrypto, createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { finalizeProviderNativeCapture } from "../dist/src/browser/chatgptConversation.js";

const root = await fs.mkdtemp(path.join(os.tmpdir(), "oracle-provider-proof-"));
const previousHome = process.env.ORACLE_HOME_DIR;
process.env.ORACLE_HOME_DIR = root;
const math = String.raw`\(\mathcal{F}_s = \sum_{n=0}^\infty \tfrac{1}{2}\,\Gamma(n)\)`;
const document = JSON.parse(
  await fs.readFile(
    new URL("../tests/fixtures/provider-conversation.json", import.meta.url),
    "utf8",
  ),
);
document.current_node = "proof";
document.mapping.proof = {
  parent: "n5",
  message: {
    id: "proof-message",
    author: { role: "assistant" },
    content: { content_type: "text", parts: [math] },
  },
};
const raw = `${JSON.stringify(document)}\n`;
const timers = [];
let requests = 0;
let challenged = false;
const context = vm.createContext({
  location: { origin: "https://chatgpt.com" },
  TextDecoder,
  TextEncoder,
  AbortSignal,
  crypto: webcrypto,
  setTimeout: (fn, ms) => {
    const timer = setTimeout(fn, ms);
    timer.unref();
    timers.push(timer);
    return timer;
  },
  fetch: async (url, options) => {
    if (url === "/api/auth/session") return Response.json({ accessToken: "synthetic-proof-token" });
    assert.equal(options.headers.Authorization, "Bearer synthetic-proof-token");
    requests++;
    if (challenged) return new Response("challenge", { status: 403 });
    return new Response(raw, { headers: { "content-type": "application/json" } });
  },
});
const Runtime = {
  evaluate: async ({ expression }) => ({
    result: { value: await vm.runInContext(expression, context) },
  }),
};
const logs = [];
const capture = (answerMarkdown, sessionId) =>
  finalizeProviderNativeCapture({
    Runtime,
    conversationId: "fixture-0001",
    answerMessageId: "proof-message",
    answerMarkdown,
    sessionId,
    logger: (line) => logs.push(line),
  });
try {
  const matched = await capture(math, "matched");
  assert.equal(matched.summary.answerFidelity, "matched");
  assert.equal(requests, 2);
  assert.equal(matched.artifacts.length, 2);
  const saved = await fs.readFile(matched.artifacts[0].path);
  assert.equal(saved.toString(), raw);
  assert.equal(createHash("sha256").update(saved).digest("hex"), matched.summary.rawSha256);
  const evidence = JSON.parse(await fs.readFile(matched.artifacts[1].path, "utf8"));
  assert.equal(evidence.materializedToDisk, true);
  assert.equal(
    evidence.independentFetch.perTurn.at(-1).sha256,
    createHash("sha256").update(math).digest("hex"),
  );
  const divergent = await capture(math.replace("_s", "*s"), "divergent");
  assert.equal(divergent.summary.answerFidelity, "divergent");
  challenged = true;
  const fallback = await capture(math, "fallback");
  assert.equal(fallback.summary.status, "unavailable");
  assert.equal(fallback.summary.failure.reason, "challenged");
  assert.equal(fallback.artifacts.length, 0);
  assert.ok(
    !JSON.stringify([matched, divergent, fallback, logs]).includes("synthetic-proof-token"),
  );
  const cli = spawnSync(
    process.execPath,
    [fileURLToPath(new URL("../dist/bin/oracle-cli.js", import.meta.url)), "--help"],
    { encoding: "utf8" },
  );
  assert.equal(cli.status, 0, cli.stderr);
  assert.match(cli.stdout, /--browser-capture-provider-native/);
  assert.match(cli.stdout, /--no-browser-capture-provider-native/);
  console.log(
    "PASS: compiled provider capture preserves recorded/math bytes, verifies independent hashes, detects notation changes, saves private evidence, and reports challenge fallback; CLI flags registered.",
  );
} finally {
  for (const timer of timers) clearTimeout(timer);
  if (previousHome === undefined) delete process.env.ORACLE_HOME_DIR;
  else process.env.ORACLE_HOME_DIR = previousHome;
  await fs.rm(root, { recursive: true, force: true });
}
