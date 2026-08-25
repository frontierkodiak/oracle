import { afterEach, describe, expect, it } from "vitest";
import http from "node:http";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readdir, writeFile } from "node:fs/promises";
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
  token = "test",
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
          authorization: `Bearer ${token}`,
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

function callRaw(
  port: number,
  route: string,
  token: string,
): Promise<{ status: number; body: Buffer }> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: "127.0.0.1",
        port,
        method: "GET",
        path: route,
        headers: { authorization: `Bearer ${token}` },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
        res.on("end", () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks) }));
      },
    );
    req.on("error", reject);
    req.end();
  });
}

async function providerCaptureArtifacts(
  root: string,
  conversationId: string,
  conversationUrl: string,
) {
  const directory = path.join(root, `capture-${conversationId}`);
  await mkdir(directory, { recursive: true });
  const rawText = JSON.stringify({ id: conversationId, source: "authoritative" });
  const independentText = JSON.stringify({ id: conversationId, source: "independent" });
  const rawSha256 = createHash("sha256").update(rawText).digest("hex");
  const independentSha256 = createHash("sha256").update(independentText).digest("hex");
  const rawPath = path.join(directory, "raw.json");
  const independentPath = path.join(directory, "independent.json");
  const evidencePath = path.join(directory, "evidence.json");
  const evidenceText = `${JSON.stringify({
    schema: "oracle.provider-native-capture-evidence/v1",
    conversation_id: conversationId,
    chatgpt_url: conversationUrl,
    materialized_document: { sha256: rawSha256, bytes: Buffer.byteLength(rawText) },
    independent_document: {
      sha256: independentSha256,
      bytes: Buffer.byteLength(independentText),
    },
    per_turn: [{ i: 0, role: "assistant", ct: "text", blen: 2, sha256_hex: "a".repeat(64) }],
  })}\n`;
  await writeFile(rawPath, rawText);
  await writeFile(independentPath, independentText);
  await writeFile(evidencePath, evidenceText);
  return [
    {
      kind: "file" as const,
      path: rawPath,
      label: "provider-native-conversation-raw",
      mimeType: "application/json",
    },
    {
      kind: "file" as const,
      path: independentPath,
      label: "provider-native-conversation-independent",
      mimeType: "application/json",
    },
    {
      kind: "file" as const,
      path: evidencePath,
      label: "provider-native-conversation-evidence",
      mimeType: "application/json",
    },
  ];
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
    expect(health.json.capabilities.features).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: "oracle.remote.maintenance-drain", version: 1 }),
        expect.objectContaining({ id: "oracle.remote.capture-grant", version: 1 }),
      ]),
    );
    expect(health.json.admission).toEqual({ state: "open" });
  });

  it("atomically fences a normal admission whose request body is still arriving", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-slow-admission-race-"));
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
    const raw = Buffer.from(JSON.stringify(payload("slow-body")));
    let finishRequest!: () => void;
    const slowResponse = new Promise<{ status: number; json: any }>((resolve, reject) => {
      const req = http.request(
        {
          host: "127.0.0.1",
          port: server!.port,
          method: "POST",
          path: "/v1/runs",
          headers: {
            authorization: "Bearer test",
            "idempotency-key": "slow-body-key",
            "content-type": "application/json",
            "content-length": raw.length,
          },
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
          res.on("end", () =>
            resolve({
              status: res.statusCode ?? 0,
              json: JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"),
            }),
          );
        },
      );
      req.on("error", reject);
      req.write(raw.subarray(0, 1));
      finishRequest = () => req.end(raw.subarray(1));
    });
    const drain = await call(
      server.port,
      "POST",
      "/v1/maintenance/drains",
      { mode: "require-idle" },
      "slow-race-drain",
    );
    expect(drain.status).toBe(201);
    finishRequest();
    await expect(slowResponse).resolves.toMatchObject({
      status: 503,
      json: { error: "admission_draining" },
    });
    expect(calls).toBe(0);
    expect((await call(server.port, "GET", "/health")).json.admission).toMatchObject({
      state: "draining",
      drainId: drain.json.drainId,
    });
  });

  it("keeps a drain and its issued scoped token effective across server restart", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-maintenance-restart-"));
    server = await createRemoteServer({
      host: "127.0.0.1",
      port: 0,
      token: "test",
      logger: () => {},
      queueHomeDir: home,
    });
    const drain = await call(
      server.port,
      "POST",
      "/v1/maintenance/drains",
      { mode: "require-idle" },
      "restart-drain-key",
    );
    const grant = await call(
      server.port,
      "POST",
      `/v1/maintenance/drains/${drain.json.drainId}/capture-grants`,
      { conversationUrl: "https://chatgpt.com/c/restart-scoped-token" },
      "restart-scoped-grant",
    );
    await server.close();
    server = await createRemoteServer({
      host: "127.0.0.1",
      port: 0,
      token: "test",
      logger: () => {},
      queueHomeDir: home,
    });
    expect((await call(server.port, "GET", "/health")).json.admission).toMatchObject({
      state: "draining",
      drainId: drain.json.drainId,
    });
    expect(
      (await call(server.port, "GET", "/health", undefined, undefined, grant.json.token)).status,
    ).toBe(200);
    expect(
      await call(server.port, "POST", "/v1/runs", payload("blocked-after-restart"), "blocked"),
    ).toMatchObject({ status: 503, json: { error: "admission_draining" } });
  });

  it("releases an idle drain even when its issued grant was never admitted", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-maintenance-unused-grant-"));
    server = await createRemoteServer({
      host: "127.0.0.1",
      port: 0,
      token: "test",
      logger: () => {},
      queueHomeDir: home,
    });
    const drain = await call(
      server.port,
      "POST",
      "/v1/maintenance/drains",
      { mode: "require-idle" },
      "unused-release-drain",
    );
    const grant = await call(
      server.port,
      "POST",
      `/v1/maintenance/drains/${drain.json.drainId}/capture-grants`,
      { conversationUrl: "https://chatgpt.com/c/unused-release-conversation" },
      "unused-release-grant",
    );
    const released = await call(
      server.port,
      "POST",
      `/v1/maintenance/drains/${drain.json.drainId}/release`,
      {},
    );
    expect(released).toMatchObject({ status: 200, json: { state: "open" } });
    expect(
      await call(server.port, "GET", `/v1/maintenance/capture-grants/${grant.json.grantId}`),
    ).toMatchObject({
      status: 200,
      json: {
        state: "revoked",
        terminal: { failureCode: "maintenance_drain_released" },
      },
    });
    expect(
      (await call(server.port, "GET", "/health", undefined, undefined, grant.json.token)).status,
    ).toBe(403);
  });

  it("scopes a one-use grant and replaces an adversarial payload with a verified capture", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-maintenance-capture-"));
    const conversationId = "accepted-conversation";
    const conversationUrl = `https://chatgpt.com/c/${conversationId}`;
    let browserCalls = 0;
    let observed: Record<string, unknown> | undefined;
    server = await createRemoteServer(
      { host: "127.0.0.1", port: 0, token: "test", logger: () => {}, queueHomeDir: home },
      {
        runBrowser: async (options) => {
          browserCalls += 1;
          observed = {
            prompt: options.prompt,
            attachments: options.attachments,
            fallbackSubmission: options.fallbackSubmission,
            followUpPrompts: options.followUpPrompts,
            sessionId: options.sessionId,
            config: options.config,
          };
          return {
            answerText: "captured",
            answerMarkdown: "captured",
            tookMs: 1,
            answerTokens: 1,
            answerChars: 8,
            promptSubmitted: false,
            conversationId,
            tabUrl: conversationUrl,
            artifacts: await providerCaptureArtifacts(home, conversationId, conversationUrl),
          };
        },
      },
    );
    const drain = await call(
      server.port,
      "POST",
      "/v1/maintenance/drains",
      { mode: "require-idle" },
      "capture-drain-key",
    );
    expect(drain.status).toBe(201);
    expect(
      (
        await call(
          server.port,
          "POST",
          `/v1/maintenance/drains/${drain.json.drainId}/capture-grants`,
          { conversationUrl: "https://evil.example/c/accepted-conversation" },
          "invalid-host-grant",
        )
      ).status,
    ).toBe(400);
    expect(
      (
        await call(
          server.port,
          "POST",
          `/v1/maintenance/drains/${drain.json.drainId}/capture-grants`,
          { conversationUrl: "https://user:secret@chatgpt.com/c/accepted-conversation" },
          "invalid-userinfo-grant",
        )
      ).status,
    ).toBe(400);
    const grant = await call(
      server.port,
      "POST",
      `/v1/maintenance/drains/${drain.json.drainId}/capture-grants`,
      { conversationUrl },
      "capture-grant-key",
    );
    expect(grant.status).toBe(201);
    expect(grant.json).toMatchObject({
      schemaVersion: 1,
      tokenType: "Bearer",
      conversationId,
    });
    expect(grant.json.token).toMatch(/^ocg1\.[0-9a-f-]{36}\.[A-Za-z0-9_-]+$/);
    const replayedGrant = await call(
      server.port,
      "POST",
      `/v1/maintenance/drains/${drain.json.drainId}/capture-grants`,
      { conversationUrl },
      "capture-grant-key",
    );
    expect(replayedGrant.status).toBe(200);
    expect(replayedGrant.json).toEqual(grant.json);
    expect(
      (await call(server.port, "GET", "/health", undefined, undefined, grant.json.token)).status,
    ).toBe(200);
    expect(
      (await call(server.port, "GET", "/v1/queue/status", undefined, undefined, grant.json.token))
        .status,
    ).toBe(403);
    expect(
      (
        await call(
          server.port,
          "GET",
          `/v1/maintenance/drains/${drain.json.drainId}`,
          undefined,
          undefined,
          grant.json.token,
        )
      ).status,
    ).toBe(403);

    const malicious = {
      prompt: "submit this instead",
      attachments: [{ hostPath: "/private/secret" }],
      browserConfig: { captureOnly: false, desiredModel: "instant" },
      options: { followUpPrompts: ["also submit this"], sessionId: "attacker" },
      unexpected: true,
    };
    const accepted = await call(
      server.port,
      "POST",
      "/v1/runs",
      malicious,
      "granted-capture-key",
      grant.json.token,
    );
    expect(accepted.status).toBe(202);
    const completed = await waitForRun(
      server.port,
      accepted.json.id,
      (run) => run.state === "completed",
    );
    expect(browserCalls).toBe(1);
    expect(observed).toMatchObject({
      prompt: "",
      attachments: [],
      sessionId: `maintenance-capture-${grant.json.grantId}`,
      config: {
        chatgptUrl: conversationUrl,
        url: conversationUrl,
        resumeConversationUrl: conversationUrl,
        captureOnly: true,
        captureProviderNative: true,
      },
    });
    expect(observed?.fallbackSubmission).toBeUndefined();
    expect(observed?.followUpPrompts).toBeUndefined();
    const observedConfig = observed?.config as Record<string, unknown> | undefined;
    expect(observedConfig?.desiredModel).toBeUndefined();
    expect(completed.result.promptSubmitted).toBe(false);
    expect(completed.result.artifacts).toHaveLength(3);

    const replay = await call(
      server.port,
      "POST",
      "/v1/runs",
      { entirely: "different and ignored" },
      "granted-capture-key",
      grant.json.token,
    );
    expect(replay).toMatchObject({ status: 202, json: { id: accepted.json.id } });
    expect(browserCalls).toBe(1);
    const secondKey = await call(
      server.port,
      "POST",
      "/v1/runs",
      {},
      "second-granted-key",
      grant.json.token,
    );
    expect(secondKey).toMatchObject({
      status: 409,
      json: { error: "capture_grant_consumed" },
    });
    expect(
      (
        await call(
          server.port,
          "GET",
          "/v1/runs/not-the-bound-run",
          undefined,
          undefined,
          grant.json.token,
        )
      ).status,
    ).toBe(403);
    const scopedEvents = await call(
      server.port,
      "GET",
      `/v1/runs/${accepted.json.id}/events?after=-1`,
      undefined,
      undefined,
      grant.json.token,
    );
    expect(scopedEvents.status).toBe(200);
    expect(scopedEvents.json.events.map((entry: any) => entry.event.type)).toEqual(
      expect.arrayContaining(["maintenance-capture-authorized", "maintenance-capture-verified"]),
    );
    expect(
      (
        await call(
          server.port,
          "POST",
          `/v1/runs/${accepted.json.id}/cancel`,
          undefined,
          undefined,
          grant.json.token,
        )
      ).status,
    ).toBe(200);
    const descriptor = completed.result.artifacts[0];
    expect(
      (
        await callRaw(
          server.port,
          `/runs/${accepted.json.id}/artifacts/${descriptor.artifactId}`,
          grant.json.token,
        )
      ).status,
    ).toBe(200);
    const receipt = await call(
      server.port,
      "GET",
      `/v1/maintenance/capture-grants/${grant.json.grantId}`,
    );
    expect(receipt.json).toMatchObject({
      state: "completed",
      runId: accepted.json.id,
      terminal: {
        state: "completed",
        verified: true,
        submissionAttempted: false,
        promptSubmitted: false,
        artifactCount: 3,
        artifactManifestSha256: expect.stringMatching(/^[a-f0-9]{64}$/),
        finalEventSeq: expect.any(Number),
      },
    });
    expect(JSON.stringify(receipt.json)).not.toContain(grant.json.token);
    const released = await call(
      server.port,
      "POST",
      `/v1/maintenance/drains/${drain.json.drainId}/release`,
      {},
    );
    expect(released).toMatchObject({ status: 200, json: { state: "open" } });
    expect(
      (await call(server.port, "GET", "/health", undefined, undefined, grant.json.token)).status,
    ).toBe(403);
  });

  it.each([
    {
      name: "submission evidence",
      result: { promptSubmitted: true, conversationId: "accepted-violation" },
      code: "maintenance_capture_prompt_submitted",
      runState: "unknown",
    },
    {
      name: "submission attempt evidence",
      result: {
        promptSubmitted: false,
        submissionAttempted: true,
        conversationId: "accepted-violation",
      },
      code: "maintenance_capture_submission_attempted",
      runState: "unknown",
    },
    {
      name: "wrong conversation",
      result: { promptSubmitted: false, conversationId: "wrong-conversation" },
      code: "maintenance_capture_wrong_conversation",
      runState: "failed",
    },
    {
      name: "missing proof artifacts",
      result: { promptSubmitted: false, conversationId: "accepted-violation" },
      code: "maintenance_capture_evidence_incomplete",
      runState: "failed",
    },
  ])("records a bridge-authored violation for $name", async ({ result, code, runState }) => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-capture-violation-"));
    const conversationId = "accepted-violation";
    const conversationUrl = `https://chatgpt.com/c/${conversationId}`;
    server = await createRemoteServer(
      { host: "127.0.0.1", port: 0, token: "test", logger: () => {}, queueHomeDir: home },
      {
        runBrowser: async () => ({
          answerText: "captured",
          answerMarkdown: "captured",
          tookMs: 1,
          answerTokens: 1,
          answerChars: 8,
          ...result,
          tabUrl: `https://chatgpt.com/c/${result.conversationId}`,
        }),
      },
    );
    const drain = await call(
      server.port,
      "POST",
      "/v1/maintenance/drains",
      { mode: "require-idle" },
      `violation-drain-${code}`,
    );
    const grant = await call(
      server.port,
      "POST",
      `/v1/maintenance/drains/${drain.json.drainId}/capture-grants`,
      { conversationUrl },
      `violation-grant-${code}`,
    );
    const accepted = await call(
      server.port,
      "POST",
      "/v1/runs",
      { ignored: true },
      `violation-run-${code}`,
      grant.json.token,
    );
    const terminalRun = await waitForRun(
      server.port,
      accepted.json.id,
      (run) => run.state === runState,
    );
    expect(terminalRun.state).toBe(runState);
    const events = await call(server.port, "GET", `/v1/runs/${accepted.json.id}/events?after=-1`);
    expect(events.json.events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          event: { type: "maintenance-capture-violation", code },
        }),
      ]),
    );
    const receipt = await call(
      server.port,
      "GET",
      `/v1/maintenance/capture-grants/${grant.json.grantId}`,
    );
    expect(receipt.json).toMatchObject({
      state: "failed",
      terminal: { state: runState, failureCode: code, verified: false },
    });
    if (code === "maintenance_capture_prompt_submitted")
      expect(receipt.json.terminal.promptSubmitted).toBe(true);
    if (code === "maintenance_capture_submission_attempted")
      expect(receipt.json.terminal.submissionAttempted).toBe(true);
    expect(
      (await call(server.port, "POST", `/v1/maintenance/drains/${drain.json.drainId}/release`, {}))
        .status,
    ).toBe(200);
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
    expect(oversized.json.error).toBe("invalid_request");
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
