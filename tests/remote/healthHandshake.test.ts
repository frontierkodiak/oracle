import { afterEach, describe, expect, test } from "vitest";
import http from "node:http";
import {
  assertSupportedNodeVersion,
  getOracleRuntimeIdentity,
  parseNodeMajor,
} from "../../src/remote/runtime.js";
import { parseHealthEnvelope } from "../../src/remote/health.js";
import { createRemoteBrowserExecutor } from "../../src/remote/client.js";

const artifact = {
  id: "oracle.remote.artifact-transfer",
  version: 1,
  limits: { maxBytes: 1024 * 1024 * 1024 },
};
const durable = {
  id: "oracle.remote.durable-queue",
  version: 1,
  limits: { maxQueued: 8, maxConcurrentRuns: 4 },
};
const envelope = (overrides: Record<string, unknown> = {}) => ({
  ok: true,
  version: "0.18.0",
  installSha: "a".repeat(40),
  runtime: { name: "node", version: "25.1.0", major: 25, minimumMajor: 24 },
  browser: { windowMode: "hidden" },
  capabilities: { schemaVersion: 1, features: [artifact, durable] },
  ...overrides,
});

describe("remote runtime and health handshake", () => {
  test.each([
    ["24.0.0", 24],
    ["25.3.1", 25],
  ])("accepts Node %s", (version, major) => {
    expect(parseNodeMajor(version)).toBe(major);
    expect(assertSupportedNodeVersion(version)).toBe(major);
  });
  test.each(["22.23.2", "23.0.0", "", "v24.0.0", "24.x"])("rejects %s", (version) => {
    if (version === "22.23.2" || version === "23.0.0")
      expect(parseNodeMajor(version)).toBe(Number(version.slice(0, 2)));
    else expect(parseNodeMajor(version)).toBeUndefined();
    expect(() => assertSupportedNodeVersion(version)).toThrow();
  });

  test("parses canonical envelope, retains unknown features, and caps artifact limits", () => {
    const parsed = parseHealthEnvelope({
      ...envelope(),
      capabilities: {
        schemaVersion: 1,
        features: [
          artifact,
          { id: "vendor.future", version: 3, limits: { maxTabs: 4, mode: "fast" } },
        ],
      },
    });
    expect(parsed?.runtime).toEqual(getOracleRuntimeIdentity("25.1.0"));
    expect(parsed?.installSha).toBe("a".repeat(40));
    expect(parsed?.browser).toEqual({ windowMode: "hidden" });
    expect(parsed?.manifest.features).toHaveLength(2);
    expect(parsed?.manifest.features[1]).toEqual({
      id: "vendor.future",
      version: 3,
      limits: { maxTabs: 4, mode: "fast" },
    });
    expect(parsed?.artifact).toEqual({
      artifactTransfer: true,
      artifactProtocolVersion: 1,
      maxArtifactBytes: 512 * 1024 * 1024,
    });
  });

  test("accepts older health envelopes without install or browser metadata", () => {
    const parsed = parseHealthEnvelope(envelope({ installSha: undefined, browser: undefined }));

    expect(parsed?.installSha).toBeUndefined();
    expect(parsed?.browser).toBeUndefined();
  });

  test.each([
    { ok: false },
    { version: "" },
    { runtime: undefined },
    { runtime: { name: "node", version: "24.0.0", major: 25, minimumMajor: 24 } },
    { runtime: { name: "node", version: "24.0.0", major: 24, minimumMajor: 23 } },
    { installSha: "not-a-sha" },
    { browser: "hidden" },
    { browser: { windowMode: "headless" } },
    {
      capabilities: {
        schemaVersion: 1,
        features: [
          { id: "x.y", version: 1 },
          { id: "x.y", version: 2 },
        ],
      },
    },
    { capabilities: { schemaVersion: 1, features: [{ id: "x.y", version: 0 }] } },
    { capabilities: { schemaVersion: 1, features: [{ id: " ", version: 1 }] } },
    { capabilities: { schemaVersion: 1, features: [{ id: "x.y", version: 1, limits: [] }] } },
    {
      capabilities: {
        schemaVersion: 1,
        features: [{ id: "x.y", version: 1, limits: { maxBytes: 0 } }],
      },
    },
  ])("rejects malformed envelope %#", (overrides) => {
    expect(parseHealthEnvelope(envelope(overrides))).toBeUndefined();
  });
});

describe("remote executor health preflight", () => {
  const servers: http.Server[] = [];
  afterEach(async () => {
    await Promise.all(
      servers
        .splice(0)
        .map((server) => new Promise<void>((resolve) => server.close(() => resolve()))),
    );
  });

  async function stub(response: { status?: number; body?: unknown; raw?: string }) {
    let healthRequests = 0;
    let runRequests = 0;
    const healthAuthorization: string[] = [];
    const server = http.createServer((req, res) => {
      if (req.method === "GET" && req.url === "/health") {
        healthRequests += 1;
        healthAuthorization.push(String(req.headers.authorization ?? ""));
        res.writeHead(response.status ?? 200, { "Content-Type": "application/json" });
        res.end(response.raw ?? JSON.stringify(response.body ?? envelope()));
        return;
      }
      if (req.method === "POST" && (req.url === "/runs" || req.url === "/v1/runs")) {
        runRequests += 1;
        req.resume();
        const now = new Date().toISOString();
        res.writeHead(req.url === "/v1/runs" ? 202 : 200, { "Content-Type": "application/json" });
        res.end(
          req.url === "/v1/runs"
            ? JSON.stringify({
                id: "11111111-1111-4111-8111-111111111111",
                state: "completed",
                phase: "terminal",
                queuePosition: 0,
                roughEtaMs: 0,
                requestHash: "a".repeat(64),
                createdAt: now,
                updatedAt: now,
                result: {
                  answerText: "ok",
                  answerMarkdown: "ok",
                  tookMs: 1,
                  answerTokens: 1,
                  answerChars: 2,
                },
              })
            : "",
        );
        return;
      }
      if (req.method === "GET" && req.url?.startsWith("/v1/runs/")) {
        res.writeHead(200, { "Content-Type": "application/json" });
        if (req.url.includes("/events")) res.end(JSON.stringify({ events: [] }));
        else {
          const now = new Date().toISOString();
          res.end(
            JSON.stringify({
              id: "11111111-1111-4111-8111-111111111111",
              state: "completed",
              phase: "terminal",
              queuePosition: 0,
              roughEtaMs: 0,
              requestHash: "a".repeat(64),
              createdAt: now,
              updatedAt: now,
              result: {
                answerText: "ok",
                answerMarkdown: "ok",
                tookMs: 1,
                answerTokens: 1,
                answerChars: 2,
              },
            }),
          );
        }
        return;
      }
      res.writeHead(404);
      res.end();
    });
    servers.push(server);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("stub did not bind");
    return {
      host: `127.0.0.1:${address.port}`,
      counts: () => ({ healthRequests, runRequests, healthAuthorization }),
    };
  }
  const run = async (host: string, requiredCapabilities?: Array<{ id: string; version: number }>) =>
    createRemoteBrowserExecutor({ host, token: "secret", requiredCapabilities })({
      prompt: "hello",
      config: {},
    });

  test("caches one authenticated health request across two runs", async () => {
    const s = await stub({});
    const executor = createRemoteBrowserExecutor({ host: s.host, token: "secret" });
    await executor({ prompt: "one", config: {} });
    await executor({ prompt: "two", config: {} });
    expect(s.counts()).toEqual({
      healthRequests: 1,
      runRequests: 2,
      healthAuthorization: ["Bearer secret"],
    });
  });

  test.each([
    { status: 401 },
    { status: 404 },
    { raw: "not json" },
    {
      body: envelope({ runtime: { name: "node", version: "23.0.0", major: 23, minimumMajor: 24 } }),
    },
    { body: envelope({ capabilities: { schemaVersion: 1, features: [] } }) },
  ])("rejects invalid health with zero runs %#", async (response) => {
    const s = await stub(response);
    await expect(
      run(s.host, [{ id: "oracle.remote.artifact-transfer", version: 1 }]),
    ).rejects.toThrow();
    expect(s.counts().runRequests).toBe(0);
  });

  test("valid required artifact capability allows a run", async () => {
    const s = await stub({});
    await run(s.host, [{ id: "oracle.remote.artifact-transfer", version: 1 }]);
    expect(s.counts().runRequests).toBe(1);
  });

  test("already-aborted signal makes zero health and runs", async () => {
    const s = await stub({});
    const controller = new AbortController();
    controller.abort();
    const executor = createRemoteBrowserExecutor({ host: s.host, token: "secret" });
    await expect(
      executor({ prompt: "no", config: {}, signal: controller.signal }),
    ).rejects.toThrow();
    expect(s.counts().healthRequests).toBe(0);
    expect(s.counts().runRequests).toBe(0);
  });
});
