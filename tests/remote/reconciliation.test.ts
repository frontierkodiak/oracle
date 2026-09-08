import { BrowserAutomationError } from "../../src/oracle/errors.js";
import { createRemoteServer } from "../../src/remote/server.js";
import { getDurableRemoteRun, reconcileDurableRemoteRun } from "../../src/remote/client.js";
import { describe, it, expect } from "vitest";
import { mkdtemp, writeFile, mkdir } from "node:fs/promises";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { DurableQueueStore } from "../../src/remote/durableQueue.js";
import { RunReconciler } from "../../src/remote/reconciliation.js";
import { persistBrowserRunArtifacts } from "../../src/remote/durableArtifacts.js";
import { TranscriptLedger, deriveChatgptProfileId } from "../../src/transcriptLedger.js";
async function fixture(dir: string, suffix: string, body = "hello") {
  const raw = {
    conversation_id: "conv-1",
    current_node: "a2",
    create_time: Date.now(),
    mapping: {
      root: { id: "root", parent: null, children: ["a1"], message: null },
      a1: {
        id: "a1",
        parent: "root",
        children: ["a2"],
        message: {
          author: { role: "user" },
          content: { content_type: "text", parts: ["question"] },
        },
      },
      a2: {
        id: "a2",
        parent: "a1",
        children: [],
        message: {
          author: { role: "assistant" },
          content: { content_type: "text", parts: [body] },
        },
      },
    },
  };
  const rawPath = path.join(dir, `${suffix}-raw.json`);
  const evidencePath = path.join(dir, `${suffix}-evidence.json`);
  const independentPath = path.join(dir, `${suffix}-independent.json`);
  const rawBytes = Buffer.from(JSON.stringify(raw));
  const digest = (value: string) => createHash("sha256").update(value).digest("hex");
  const decimal = (value: string) => [...Buffer.from(value, "hex")];
  const evidence = {
    schema: "oracle.provider-native-capture-evidence/v1",
    conversation_id: "conv-1",
    materialized_document: {
      sha256: createHash("sha256").update(rawBytes).digest("hex"),
      bytes: rawBytes.byteLength,
    },
    independent_document: {
      sha256: createHash("sha256").update(rawBytes).digest("hex"),
      bytes: rawBytes.byteLength,
    },
    independent_fetch: {
      document_sha256_decimal_bytes: decimal(
        createHash("sha256").update(rawBytes).digest("hex"),
      ).join(" "),
      document_bytes: rawBytes.byteLength,
      fetched_at: "2026-01-01T00:00:00.000Z",
    },
    per_turn: [
      {
        i: 0,
        role: "user",
        ct: "text",
        blen: Buffer.byteLength("question"),
        sha256_hex: digest("question"),
        sha256_dec: decimal(digest("question")).join(" "),
        attachments: [],
      },
      {
        i: 1,
        role: "assistant",
        ct: "text",
        blen: Buffer.byteLength(body),
        sha256_hex: digest(body),
        sha256_dec: decimal(digest(body)).join(" "),
        attachments: [],
      },
    ],
  };
  Object.assign(evidence, { chatgpt_url: "https://chatgpt.com/c/conv-1" });
  await writeFile(rawPath, rawBytes);
  await writeFile(evidencePath, JSON.stringify(evidence));
  await writeFile(independentPath, rawBytes);
  return { rawPath, evidencePath, independentPath, rawBytes };
}

async function setup() {
  const homeDir = await mkdtemp(path.join(os.tmpdir(), "oracle-reconcile-"));
  let now = 1000000;
  let queue = await DurableQueueStore.open({ homeDir, capacity: 1, backlog: 0, now: () => now });
  const make = () =>
    new RunReconciler(queue, {
      enabled: true,
      profileId: deriveChatgptProfileId({}),
      now: () => now,
    });
  const original = await queue.submit("original", {
    prompt: "NEVER RESEND",
    browserConfig: { url: "https://chatgpt.com/c/other" },
  });
  queue.claimNext();
  queue.bindProfile(original.id, deriveChatgptProfileId({}));
  queue.transition(original.id, "running", "prompt_submitted", {
    runtimeHint: { conversationId: "conv-1", promptSubmitted: true },
  });
  queue.close();
  queue = await DurableQueueStore.open({ homeDir, capacity: 1, backlog: 0, now: () => now });
  return {
    homeDir,
    get queue() {
      return queue;
    },
    original,
    make,
    advance: () => {
      now += 1000000;
    },
    restart: async () => {
      queue.close();
      queue = await DurableQueueStore.open({ homeDir, capacity: 1, backlog: 0, now: () => now });
    },
  };
}
async function capture(ctx: Awaited<ReturnType<typeof setup>>, wrong = false) {
  const record = ctx.make().get(ctx.original.id)!;
  const child = ctx.queue.claimNext()!;
  expect(child.id).toBe(record.captureRunId);
  const files = await fixture(ctx.homeDir, child.id);
  await persistBrowserRunArtifacts({
    queueRoot: ctx.queue.root,
    runId: child.id,
    result: {
      answerText: "",
      answerMarkdown: "",
      tookMs: 1,
      answerTokens: 0,
      answerChars: 0,
      promptSubmitted: false,
      conversationId: wrong ? "other" : "conv-1",
      artifacts: [
        { kind: "file", path: files.rawPath, label: "provider-native-conversation-raw" },
        { kind: "file", path: files.evidencePath, label: "provider-native-conversation-evidence" },
        {
          kind: "file",
          path: files.independentPath,
          label: "provider-native-conversation-independent",
        },
      ],
    } as any,
  });
  return child;
}
describe("durable read-only reconciliation", () => {
  it.each([
    ["auth-session-unavailable", "auth_unavailable"],
    ["challenged", "challenged"],
  ])("preserves real capture-only %s failure through HTTP", async (reason, state) => {
    const ctx = await setup();
    ctx.queue.close();
    const server = await createRemoteServer(
      {
        host: "127.0.0.1",
        port: 0,
        token: "secret",
        queueHomeDir: ctx.homeDir,
        allowCaptureOnly: true,
        logger: () => {},
      },
      {
        runBrowser: async () => {
          throw new BrowserAutomationError("capture unavailable", {
            stage: "capture-only",
            details: { failure: { reason } },
          });
        },
      },
    );
    try {
      let receipt;
      for (let i = 0; i < 100; i++) {
        receipt = await reconcileDurableRemoteRun(
          `127.0.0.1:${server.port}`,
          ctx.original.id,
          "secret",
          true,
        );
        if (receipt?.state === state) break;
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      expect(receipt?.state).toBe(state);
      expect(receipt?.nextRetryAt).toBeDefined();
    } finally {
      await server.close();
    }
  });
  it("never dispatches a queued recovery under a changed host profile even when the parent link was lost", async () => {
    const ctx = await setup();
    await ctx.make().sweep();
    const record = ctx.make().get(ctx.original.id)!;
    delete record.captureRunId;
    ctx.queue.saveReconciliation(ctx.original.id, record);
    ctx.queue.close();
    let calls = 0;
    const server = await createRemoteServer(
      {
        host: "127.0.0.1",
        port: 0,
        token: "secret",
        queueHomeDir: ctx.homeDir,
        allowCaptureOnly: true,
        manualLoginProfileDir: path.join(ctx.homeDir, "different-profile"),
        logger: () => {},
      },
      {
        runBrowser: async () => {
          calls++;
          throw new Error("must not run");
        },
      },
    );
    try {
      const receipt = await reconcileDurableRemoteRun(
        `127.0.0.1:${server.port}`,
        ctx.original.id,
        "secret",
        true,
      );
      expect(receipt?.state).toBe("profile_mismatch");
      expect(calls).toBe(0);
    } finally {
      await server.close();
    }
  });
  it("recovers admission commit before child linkage without admitting a duplicate", async () => {
    const ctx = await setup();
    const submit = ctx.queue.submit.bind(ctx.queue);
    let childId = "";
    ctx.queue.submit = async (...args) => {
      const child = await submit(...args);
      childId = child.id;
      throw new Error("lost admission response");
    };
    await expect(ctx.make().sweep()).rejects.toThrow("lost admission response");
    await ctx.restart();
    await ctx.make().sweep();
    expect(ctx.make().get(ctx.original.id)?.captureRunId).toBe(childId);
    expect(ctx.queue.status().queued).toBe(1);
    expect(ctx.make().get(ctx.original.id)?.attempt).toBe(1);
    ctx.queue.close();
  });
  it("pauses canceled captures and resumes only after explicit operator action", async () => {
    const ctx = await setup();
    await ctx.make().sweep();
    const childId = ctx.make().get(ctx.original.id)!.captureRunId!;
    ctx.queue.cancel(childId);
    await ctx.make().sweep();
    expect(ctx.make().get(ctx.original.id)?.state).toBe("paused");
    await ctx.restart();
    await ctx.make().sweep();
    expect(ctx.queue.status().queued).toBe(0);
    await ctx.make().resume(ctx.original.id);
    await ctx.make().sweep();
    expect(ctx.make().get(ctx.original.id)?.captureRunId).not.toBe(childId);
    expect(ctx.queue.status().queued).toBe(1);
    ctx.queue.close();
  });
  it("requires deliberate binding for historical profile provenance", async () => {
    const ctx = await setup();
    const original = await ctx.queue.submit("historical", { prompt: "old", browserConfig: {} });
    ctx.queue.claimNext();
    ctx.queue.transition(original.id, "unknown", "terminal", {
      runtimeHint: { conversationId: "conv-1" },
    });
    expect((await ctx.make().request(original.id)).state).toBe("profile_unbound");
    expect((await ctx.make().resume(original.id)).state).toBe("profile_unbound");
    expect((await ctx.make().resume(original.id, true)).profileBinding).toBe(
      "operator_current_profile",
    );
    ctx.queue.close();
  });
  it("exposes a separate authenticated API, keeps v1 clients valid, and invokes only the original stored conversation", async () => {
    const ctx = await setup();
    const originalId = ctx.original.id;
    ctx.queue.close();
    let calls = 0;
    const server = await createRemoteServer(
      {
        host: "127.0.0.1",
        port: 0,
        token: "secret",
        queueHomeDir: ctx.homeDir,
        allowCaptureOnly: true,
        maxConcurrentRuns: 1,
        logger: () => {},
      },
      {
        runBrowser: async (input) => {
          calls++;
          expect(input.prompt).toBe("");
          expect(input.config?.captureOnly).toBe(true);
          expect(input.config?.resumeConversationUrl).toBe("https://chatgpt.com/c/conv-1");
          const files = await fixture(ctx.homeDir, "http");
          return {
            answerText: "",
            answerMarkdown: "",
            tookMs: 1,
            answerTokens: 0,
            answerChars: 0,
            conversationId: "conv-1",
            promptSubmitted: false,
            artifacts: [
              { kind: "file", path: files.rawPath, label: "provider-native-conversation-raw" },
              {
                kind: "file",
                path: files.evidencePath,
                label: "provider-native-conversation-evidence",
              },
              {
                kind: "file",
                path: files.independentPath,
                label: "provider-native-conversation-independent",
              },
            ],
          };
        },
      },
    );
    try {
      const host = `127.0.0.1:${server.port}`;
      const old = await getDurableRemoteRun(host, originalId, "secret");
      expect(old.state).toBe("unknown");
      expect(old).not.toHaveProperty("reconciliation");
      let receipt = await reconcileDurableRemoteRun(host, originalId, "secret");
      for (let i = 0; i < 100 && receipt?.state !== "captured_unattributed"; i++) {
        await new Promise((resolve) => setTimeout(resolve, 10));
        receipt = await reconcileDurableRemoteRun(host, originalId, "secret", true);
      }
      expect(receipt?.state).toBe("captured_unattributed");
      expect(calls).toBe(1);
      await expect(reconcileDurableRemoteRun(host, originalId, "wrong")).rejects.toThrow();
      const bad = await fetch(`http://${host}/v1/runs/${originalId}/reconciliation`, {
        method: "POST",
        headers: { authorization: "Bearer secret" },
        body: JSON.stringify({ conversationId: "other" }),
      });
      expect(bad.status).toBe(400);
    } finally {
      await server.close();
    }
  });
  it("collects after accepted-prompt restart, reuses files after a second crash, preserves historical unknown", async () => {
    const ctx = await setup();
    const recon = ctx.make();
    await Promise.all([recon.sweep(), recon.sweep()]);
    const record = recon.get(ctx.original.id)!;
    expect(record.attempt).toBe(1);
    expect(await ctx.queue.request(record.captureRunId!)).toEqual({
      prompt: "",
      attachments: [],
      browserConfig: {
        chatgptUrl: "https://chatgpt.com/c/conv-1",
        url: "https://chatgpt.com/c/conv-1",
        resumeConversationUrl: "https://chatgpt.com/c/conv-1",
        captureOnly: true,
        captureProviderNative: true,
      },
      options: {},
    });
    const child = await capture(ctx);
    await ctx.restart();
    await ctx.make().sweep();
    const result = ctx.make().get(ctx.original.id)!;
    expect(result.state).toBe("captured_unattributed");
    expect(result.evidence?.observationId).toBe(record.observationId);
    expect(ctx.queue.get(ctx.original.id)?.state).toBe("unknown");
    expect(ctx.queue.get(child.id)?.state).toBe("unknown");
    expect(ctx.queue.status().active).toBe(0);
    await ctx.make().sweep();
    expect(ctx.make().get(ctx.original.id)?.attempt).toBe(1);
    ctx.queue.close();
  });
  it("replays one ledger observation after a crash between ledger commit and resolution", async () => {
    const ctx = await setup();
    await ctx.make().sweep();
    const child = await capture(ctx);
    ctx.queue.transition(child.id, "completed", "terminal");
    const save = ctx.queue.saveReconciliation.bind(ctx.queue);
    let crashed = false;
    ctx.queue.saveReconciliation = (id, record: any) => {
      if (!crashed && record.state === "captured_unattributed") {
        crashed = true;
        throw new Error("crash after ledger");
      }
      save(id, record);
    };
    await expect(ctx.make().sweep()).rejects.toThrow("crash after ledger");
    await ctx.restart();
    await ctx.make().sweep();
    expect(ctx.make().get(ctx.original.id)?.state).toBe("captured_unattributed");
    const ledger = await TranscriptLedger.open({
      root: path.join(ctx.homeDir, "transcript-ledger"),
    });
    expect(ledger.list()[0]?.observationCount).toBe(1);
    ledger.close();
    ctx.queue.close();
  });
  it("persists challenge backoff, exhausts bounded fresh captures without resubmission", async () => {
    const ctx = await setup();
    for (let attempt = 1; attempt <= 3; attempt++) {
      await ctx.make().sweep();
      const child = ctx.queue.claimNext()!;
      expect((await ctx.queue.request(child.id))?.prompt).toBe("");
      ctx.queue.transition(child.id, "failed", "terminal", { error: "Cloudflare challenged" });
      await ctx.make().sweep();
      expect(ctx.make().get(ctx.original.id)?.state).toBe(
        attempt === 3 ? "retry_exhausted" : "challenged",
      );
      await ctx.restart();
      await ctx.make().sweep();
      expect(ctx.queue.status().queued).toBe(0);
      ctx.advance();
    }
    await ctx.make().sweep();
    expect(ctx.queue.status().queued).toBe(0);
    expect(ctx.make().get(ctx.original.id)?.attempts).toHaveLength(3);
    ctx.queue.close();
  });
  it("requires stored identity and excludes capture-only originals", async () => {
    const ctx = await setup();
    const original = await ctx.queue.submit("no-id", { prompt: "x", browserConfig: {} });
    ctx.queue.claimNext();
    ctx.queue.transition(original.id, "unknown", "terminal");
    expect((await ctx.make().request(original.id)).state).toBe("missing_identity");
    const child = await ctx.queue.submit("capture", {
      prompt: "",
      browserConfig: { captureOnly: true },
    });
    ctx.queue.claimNext();
    ctx.queue.transition(child.id, "unknown", "terminal");
    expect((await ctx.make().request(child.id)).state).toBe("ineligible");
    ctx.queue.close();
  });
  it("rejects other-conversation evidence and incomplete crash artifacts without wedging retries", async () => {
    const ctx = await setup();
    await ctx.make().sweep();
    const child = await capture(ctx, true);
    ctx.queue.transition(child.id, "completed", "terminal");
    await ctx.make().sweep();
    expect(ctx.make().get(ctx.original.id)?.state).toBe("retry_wait");
    ctx.advance();
    await ctx.make().sweep();
    const retry = ctx.queue.claimNext()!;
    expect(retry.id).not.toBe(child.id);
    await mkdir(path.join(ctx.queue.runDirectory(retry.id), "artifacts"));
    await ctx.restart();
    await ctx.make().sweep();
    ctx.advance();
    await ctx.make().sweep();
    expect(ctx.make().get(ctx.original.id)?.captureRunId).not.toBe(retry.id);
    ctx.queue.close();
  });
  it("respects maintenance drains and queue capacity, never consumes a grant", async () => {
    const ctx = await setup();
    const drain = ctx.queue.beginDrainIfIdle("drain", "require-idle");
    const grant = ctx.queue.issueCaptureGrant(
      drain.drainId,
      "conv-1",
      "https://chatgpt.com/c/conv-1",
      "grant",
      "authority",
    );
    await ctx.make().sweep();
    expect(ctx.queue.status().queued).toBe(0);
    expect(ctx.queue.authorizeCaptureGrant(grant.token)?.state).toBe("issued");
    ctx.queue.releaseDrain(drain.drainId);
    const normal = await ctx.queue.submit("normal", { prompt: "new", browserConfig: {} });
    await ctx.make().sweep();
    expect(ctx.queue.status().queued).toBe(1);
    expect(ctx.make().get(ctx.original.id)?.captureRunId).toBeUndefined();
    ctx.queue.cancel(normal.id);
    await ctx.make().sweep();
    expect(ctx.queue.status().queued).toBe(1);
    ctx.queue.close();
  });
});
