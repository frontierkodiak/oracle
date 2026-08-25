import { afterEach, describe, expect, it } from "vitest";
import http from "node:http";
import { mkdtemp, readdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createRemoteServer } from "../../src/remote/server.js";
import { getDurableRemoteRun } from "../../src/remote/client.js";
import { BrowserAutomationError } from "../../src/oracle/errors.js";

const payload = (prompt: string) => ({ prompt, attachments: [], browserConfig: {}, options: {} });

function call(
  port: number,
  method: string,
  route: string,
  body?: unknown,
  key?: string,
): Promise<{ status: number; json: any }> {
  return new Promise((resolve, reject) => {
    const raw = body === undefined ? undefined : Buffer.from(JSON.stringify(body));
    const req = http.request(
      {
        host: "127.0.0.1",
        port,
        method,
        path: route,
        headers: {
          authorization: "Bearer test",
          ...(raw ? { "content-type": "application/json", "content-length": raw.length } : {}),
          ...(key ? { "idempotency-key": key } : {}),
        },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c) => chunks.push(Buffer.from(c)));
        res.on("end", () =>
          resolve({
            status: res.statusCode ?? 0,
            json: JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"),
          }),
        );
      },
    );
    req.on("error", reject);
    if (raw) req.write(raw);
    req.end();
  });
}

async function waitForRun(
  port: number,
  id: string,
  predicate: (run: any) => boolean,
): Promise<any> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const response = await call(port, "GET", `/v1/runs/${id}`);
    if (predicate(response.json)) return response.json;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`durable run ${id} did not reach the expected state`);
}

describe("durable remote server admission", () => {
  let server: Awaited<ReturnType<typeof createRemoteServer>> | undefined;
  afterEach(async () => {
    await server?.close();
    server = undefined;
  });

  it("admits four, persists the fifth at position one, and cancels queued work", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-durable-server-"));
    let started = 0;
    server = await createRemoteServer(
      { host: "127.0.0.1", port: 0, token: "test", logger: () => {}, queueHomeDir: home },
      {
        runBrowser: async ({ signal }) => {
          started += 1;
          await new Promise<void>((resolve) =>
            signal?.addEventListener("abort", () => resolve(), { once: true }),
          );
          return {
            answerText: "ok",
            answerMarkdown: "ok",
            tookMs: 1,
            answerTokens: 1,
            answerChars: 2,
          };
        },
      },
    );
    const accepted = [];
    for (let i = 0; i < 5; i++)
      accepted.push(await call(server.port, "POST", "/v1/runs", payload(String(i)), `key-${i}`));
    expect(accepted.every((r) => r.status === 202)).toBe(true);
    expect(started).toBe(4);
    expect(accepted[4]?.json.queuePosition).toBe(1);
    expect(accepted[4]?.json.roughEtaMs).toBeGreaterThanOrEqual(300_000);
    const canceled = await call(server.port, "POST", `/v1/runs/${accepted[4]?.json.id}/cancel`);
    expect(canceled.status).toBe(200);
    expect(canceled.json.state).toBe("canceled");
    const health = await call(server.port, "GET", "/health");
    expect(
      health.json.capabilities.features.find(
        (feature: any) => feature.id === "oracle.remote.durable-queue",
      ).limits,
    ).toEqual({ maxQueued: 8, maxConcurrentRuns: 4 });
  });

  it("advertises configured queue capacity and backlog rather than fixed limits", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-health-capacity-"));
    server = await createRemoteServer(
      {
        host: "127.0.0.1",
        port: 0,
        token: "test",
        logger: () => {},
        queueHomeDir: home,
        maxConcurrentRuns: 2,
        maxQueuedRuns: 3,
      },
      {
        runBrowser: async () => ({
          answerText: "ok",
          answerMarkdown: "ok",
          tookMs: 1,
          answerTokens: 1,
          answerChars: 2,
        }),
      },
    );
    const health = await call(server.port, "GET", "/health");
    const feature = health.json.capabilities.features.find(
      (item: any) => item.id === "oracle.remote.durable-queue",
    );
    expect(feature.limits).toEqual({ maxQueued: 3, maxConcurrentRuns: 2 });
    expect(health.json.queue).toMatchObject({ capacity: 2, backlog: 3 });
  });

  it("removes the legacy endpoint without invoking the browser", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-legacy-server-"));
    let calls = 0;
    server = await createRemoteServer(
      { host: "127.0.0.1", port: 0, token: "test", logger: () => {}, queueHomeDir: home },
      {
        runBrowser: async () => {
          calls += 1;
          throw new Error("must not run");
        },
      },
    );
    const response = await call(server.port, "POST", "/runs", payload("legacy"), "legacy-key");
    expect(response.status).toBe(410);
    expect(calls).toBe(0);
    expect(await readdir(path.join(home, "remote-queue", "runs"))).toEqual([]);
  });

  it("rejects malformed durable payloads before creating run residue", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-malformed-server-"));
    let calls = 0;
    server = await createRemoteServer(
      { host: "127.0.0.1", port: 0, token: "test", logger: () => {}, queueHomeDir: home },
      {
        runBrowser: async () => {
          calls += 1;
          throw new Error("must not run");
        },
      },
    );
    const malformed = [
      [payload("unknown"), { "x-unknown": true }],
      [{ ...payload("negative"), browserConfig: { timeoutMs: -1 } }, undefined],
      [{ ...payload("nonfinite"), browserConfig: { timeoutMs: null } }, undefined],
      [
        {
          ...payload("base64"),
          attachments: [{ fileName: "a", displayPath: "a", sizeBytes: 2, contentBase64: "YQ==" }],
        },
        undefined,
      ],
      [
        {
          ...payload("aggregate"),
          attachments: Array.from({ length: 129 }, () => ({
            fileName: "a",
            displayPath: "a",
            sizeBytes: 0,
            contentBase64: "",
          })),
        },
        undefined,
      ],
    ] as const;
    for (const [body, extra] of malformed) {
      const request = extra ? { ...(body as any), ...extra } : body;
      const response = await call(server.port, "POST", "/v1/runs", request, `bad-${Math.random()}`);
      expect(response.status).toBe(400);
    }
    const oversized = await call(
      server.port,
      "POST",
      "/v1/runs",
      payload("oversized"),
      "k".repeat(513),
    );
    expect(oversized.status).toBe(400);
    expect(calls).toBe(0);
    expect(await readdir(path.join(home, "remote-queue", "runs"))).toEqual([]);
  });

  it("rejects capture-only before admission unless the host explicitly enables it", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-capture-gated-server-"));
    let calls = 0;
    server = await createRemoteServer(
      { host: "127.0.0.1", port: 0, token: "test", logger: () => {}, queueHomeDir: home },
      {
        runBrowser: async () => {
          calls += 1;
          throw new Error("must not run");
        },
      },
    );
    const rejected = await call(
      server.port,
      "POST",
      "/v1/runs",
      { ...payload("capture"), browserConfig: { captureOnly: true } },
      "capture-disabled",
    );
    expect(rejected.status).toBe(400);
    expect(rejected.json.error).toBe("capture_only_disabled");
    expect(calls).toBe(0);
    expect(await readdir(path.join(home, "remote-queue", "runs"))).toEqual([]);
    const disabledHealth = await call(server.port, "GET", "/health");
    expect(disabledHealth.json.capabilities.features).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ id: "oracle.browser.capture-only" })]),
    );

    await server.close();
    server = await createRemoteServer(
      {
        host: "127.0.0.1",
        port: 0,
        token: "test",
        logger: () => {},
        queueHomeDir: await mkdtemp(path.join(os.tmpdir(), "oracle-capture-enabled-server-")),
        allowCaptureOnly: true,
      },
      {
        runBrowser: async () => ({
          answerText: "captured",
          answerMarkdown: "captured",
          tookMs: 1,
          answerTokens: 1,
          answerChars: 8,
          promptSubmitted: false,
        }),
      },
    );
    const health = await call(server.port, "GET", "/health");
    expect(health.json.capabilities.features).toEqual(
      expect.arrayContaining([expect.objectContaining({ id: "oracle.browser.capture-only" })]),
    );
  });

  it("exposes conversation-safe runtime hints but never host hints or raw browser logs", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-public-events-"));
    const secretPath = "/Users/carbon/.oracle/browser-profile/Default";
    server = await createRemoteServer(
      { host: "127.0.0.1", port: 0, token: "test", logger: () => {}, queueHomeDir: home },
      {
        runBrowser: async ({ runtimeHintCb, log }) => {
          log?.(`Chrome profile: ${secretPath}`);
          await runtimeHintCb?.(
            {
              chromePid: 123,
              chromePort: 9222,
              chromeHost: "127.0.0.1",
              userDataDir: secretPath,
              chromeTargetId: "host-target",
              tabUrl: "https://chatgpt.com/c/public-conversation",
              conversationId: "public-conversation",
              submissionAttempted: true,
              promptSubmitted: true,
              controllerPid: 456,
            },
            {
              requestedModel: "gpt-5.6-sol",
              resolvedLabel: "GPT-5.6 Sol",
              status: "already-selected",
              verified: true,
              source: "config",
              capturedAt: "2026-08-25T00:00:00.000Z",
              path: secretPath,
            } as never,
          );
          return {
            answerText: "ok",
            answerMarkdown: "ok",
            tookMs: 1,
            answerTokens: 1,
            answerChars: 2,
            promptSubmitted: true,
          };
        },
      },
    );
    const accepted = await call(server.port, "POST", "/v1/runs", payload("public"), "public-key");
    const completed = await waitForRun(
      server.port,
      accepted.json.id,
      (run) => run.state === "completed",
    );
    const events = await call(server.port, "GET", `/v1/runs/${accepted.json.id}/events?after=-1`);
    const wire = JSON.stringify({ completed, events: events.json });
    expect(completed.runtimeHint).toEqual({
      tabUrl: "https://chatgpt.com/c/public-conversation",
      conversationId: "public-conversation",
      submissionAttempted: true,
      promptSubmitted: true,
      modelSelection: expect.objectContaining({ resolvedLabel: "GPT-5.6 Sol" }),
    });
    expect(wire).not.toContain(secretPath);
    expect(wire).not.toContain("chromePid");
    expect(wire).not.toContain("chromeTargetId");
    expect(events.json.events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          event: { type: "log", message: "Browser progress updated" },
        }),
      ]),
    );
  });

  it("fails closed when cancellation wins before the irreversible submit marker", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-cancel-before-send-"));
    let releaseAttempt!: () => void;
    let started!: () => void;
    let attemptFinished!: () => void;
    const attemptGate = new Promise<void>((resolve) => (releaseAttempt = resolve));
    const startedGate = new Promise<void>((resolve) => (started = resolve));
    const attemptFinishedGate = new Promise<void>((resolve) => (attemptFinished = resolve));
    let dispatched = 0;
    server = await createRemoteServer(
      { host: "127.0.0.1", port: 0, token: "test", logger: () => {}, queueHomeDir: home },
      {
        runBrowser: async ({ runtimeHintCb }) => {
          started();
          await attemptGate;
          try {
            await runtimeHintCb?.({ submissionAttempted: true, promptSubmitted: false });
            dispatched += 1;
            return {
              answerText: "must not dispatch",
              answerMarkdown: "must not dispatch",
              tookMs: 1,
              answerTokens: 1,
              answerChars: 17,
            };
          } finally {
            attemptFinished();
          }
        },
      },
    );
    const accepted = await call(server.port, "POST", "/v1/runs", payload("cancel"), "cancel-key");
    await startedGate;
    const canceled = await call(server.port, "POST", `/v1/runs/${accepted.json.id}/cancel`);
    releaseAttempt();
    await attemptFinishedGate;
    await waitForRun(server.port, accepted.json.id, (run) => run.state === "canceled");
    expect(canceled.json.cancellation.outcome).toBe("canceled");
    expect(dispatched).toBe(0);
  });

  it("leaves a client-parseable terminal state across graceful shutdown and reopen", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-graceful-shutdown-"));
    server = await createRemoteServer(
      { host: "127.0.0.1", port: 0, token: "test", logger: () => {}, queueHomeDir: home },
      {
        runBrowser: async ({ signal }) => {
          if (!signal?.aborted)
            await new Promise<void>((resolve) =>
              signal?.addEventListener("abort", () => resolve(), { once: true }),
            );
          throw new Error("supervisor abort");
        },
      },
    );
    const accepted = await call(
      server.port,
      "POST",
      "/v1/runs",
      payload("shutdown"),
      "shutdown-key",
    );
    await waitForRun(server.port, accepted.json.id, (run) => run.state === "running");
    await server.close();
    server = await createRemoteServer(
      { host: "127.0.0.1", port: 0, token: "test", logger: () => {}, queueHomeDir: home },
      { runBrowser: async () => Promise.reject(new Error("must not restart terminal work")) },
    );
    await expect(
      getDurableRemoteRun(`127.0.0.1:${server.port}`, accepted.json.id, "test"),
    ).resolves.toMatchObject({
      state: "failed",
      failure: { code: "server_shutdown_before_submit", type: "failed" },
    });
  });

  it("surfaces typed throttling without exposing host paths or exception text", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-safe-failure-"));
    const secret = "/Users/carbon/.oracle/browser-profile bearer-super-secret";
    server = await createRemoteServer(
      { host: "127.0.0.1", port: 0, token: "test", logger: () => {}, queueHomeDir: home },
      {
        runBrowser: async () => {
          throw new BrowserAutomationError(`provider failed at ${secret}`, {
            stage: "chatgpt-throttled",
          });
        },
      },
    );
    const accepted = await call(
      server.port,
      "POST",
      "/v1/runs",
      payload("throttle"),
      "throttle-key",
    );
    const failed = await waitForRun(server.port, accepted.json.id, (run) => run.state === "failed");
    expect(failed.failure).toEqual({
      type: "chatgpt-throttled",
      message: "ChatGPT rate limiting is active for this account; retry later.",
    });
    expect(JSON.stringify(failed)).not.toContain(secret);
    expect(JSON.stringify(failed)).not.toContain("bearer-super-secret");
  });
});
