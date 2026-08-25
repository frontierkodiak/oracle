import { describe, expect, it } from "vitest";
import { mkdtemp, readFile, stat, symlink } from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { setOracleHomeDirOverrideForTest } from "../../src/oracleHome.js";
import {
  createRemoteBrowserExecutor,
  DurableSubmissionUnknownError,
  getDurableRemoteQueueStatus,
  readDurableReceipt,
  receiptPath,
  getDurableRemoteRunEvents,
  watchDurableRemoteRun,
  writeDurableReceipt,
  submitDurableRemoteRun,
  submitDurableRemoteRunWithReceipt,
} from "../../src/remote/client.js";

const runSnapshot = (id: string, state: "queued" | "completed" = "completed") => ({
  id,
  state,
  phase: state === "completed" ? "terminal" : "accepted",
  queuePosition: 0,
  roughEtaMs: 0,
  requestHash: "a".repeat(64),
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:01.000Z",
  ...(state === "completed"
    ? {
        result: {
          answerText: "ok",
          answerMarkdown: "ok",
          tookMs: 1,
          answerTokens: 1,
          answerChars: 2,
        },
      }
    : {}),
});

const health = () => ({
  ok: true,
  version: "1",
  runtime: { name: "node", version: "25.1.0", major: 25, minimumMajor: 24 },
  capabilities: {
    schemaVersion: 1,
    features: [
      { id: "oracle.remote.durable-queue", version: 1 },
      { id: "oracle.browser.capture-only", version: 1 },
    ],
  },
});

async function listen(handler: (req: http.IncomingMessage, res: http.ServerResponse) => void) {
  const server = http.createServer(handler);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { server, host: `127.0.0.1:${(server.address() as any).port}` };
}

async function close(server: http.Server) {
  await new Promise<void>((resolve) => server.close(() => resolve()));
}

async function body(req: http.IncomingMessage): Promise<any> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(Buffer.from(chunk));
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

describe("durable remote client receipts", () => {
  it("persists the receipt before authenticated explicit submission", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-explicit-submit-"));
    setOracleHomeDirOverrideForTest(home);
    const seen: { key?: string; auth?: string } = {};
    const server = http.createServer((req, res) => {
      if (req.url === "/health") return void res.end(JSON.stringify(health()));
      seen.key = String(req.headers["idempotency-key"]);
      seen.auth = String(req.headers.authorization);
      res.end(JSON.stringify(runSnapshot("run-explicit", "queued")));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    try {
      const result = await submitDurableRemoteRunWithReceipt({
        host: `127.0.0.1:${(server.address() as any).port}`,
        token: "secret-token",
        sessionId: "explicit-session",
        payload: {
          prompt: "hello",
          attachments: [],
          browserConfig: {} as any,
          options: { sessionId: "explicit-session" },
        },
      });
      expect(result.snapshot.id).toBe("run-explicit");
      expect(seen.key).toMatch(/^[a-f0-9]{64}$/);
      expect(seen.auth).toBe("Bearer secret-token");
      expect(await readDurableReceipt("explicit-session")).toMatchObject({
        runId: "run-explicit",
        idempotencyKey: seen.key,
      });
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      setOracleHomeDirOverrideForTest(null);
    }
  });

  it("marks an explicit submission unknown after two ambiguous responses and reuses its key", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-explicit-unknown-"));
    setOracleHomeDirOverrideForTest(home);
    let posts = 0;
    const keys: string[] = [];
    const { server, host } = await listen(async (req, res) => {
      if (req.url === "/health") return void res.end(JSON.stringify(health()));
      if (req.method === "POST" && req.url === "/v1/runs") {
        posts += 1;
        keys.push(String(req.headers["idempotency-key"]));
        await body(req);
        if (posts <= 2) return void res.destroy();
        return void res.end(JSON.stringify(runSnapshot("explicit-recovered", "queued")));
      }
      res.statusCode = 404;
      res.end();
    });
    const request = {
      host,
      sessionId: "explicit-unknown",
      payload: {
        prompt: "hello",
        attachments: [],
        browserConfig: {} as any,
        options: { sessionId: "explicit-unknown" },
      },
    };
    try {
      await expect(submitDurableRemoteRunWithReceipt(request)).rejects.toBeInstanceOf(
        DurableSubmissionUnknownError,
      );
      expect(await readDurableReceipt(request.sessionId)).toMatchObject({ submission: "unknown" });
      const recovered = await submitDurableRemoteRunWithReceipt(request);
      expect(recovered.snapshot.id).toBe("explicit-recovered");
      expect(posts).toBe(3);
      expect(new Set(keys).size).toBe(1);
      expect(await readDurableReceipt(request.sessionId)).toMatchObject({
        runId: "explicit-recovered",
      });
    } finally {
      await close(server);
      setOracleHomeDirOverrideForTest(null);
    }
  });

  it("writes an atomic private receipt and reopens the same key", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-client-"));
    setOracleHomeDirOverrideForTest(home);
    try {
      const receipt = { sessionId: "session-1", idempotencyKey: "a".repeat(64), runId: "run-1" };
      await writeDurableReceipt(receipt);
      expect(await readDurableReceipt("session-1")).toEqual(receipt);
      expect((await stat(receiptPath("session-1"))).mode & 0o777).toBe(0o600);
      expect(JSON.parse(await readFile(receiptPath("session-1"), "utf8"))).toEqual(receipt);
    } finally {
      setOracleHomeDirOverrideForTest(null);
    }
  });

  it("rejects malformed receipt keys", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-client-"));
    setOracleHomeDirOverrideForTest(home);
    try {
      await expect(
        writeDurableReceipt({ sessionId: "session-2", idempotencyKey: "bad" }),
      ).rejects.toThrow();
    } finally {
      setOracleHomeDirOverrideForTest(null);
    }
  });

  it("rejects a symlink receipt", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-client-"));
    setOracleHomeDirOverrideForTest(home);
    try {
      const target = receiptPath("session-3");
      const dir = path.dirname(target);
      await (await import("node:fs/promises")).mkdir(dir, { recursive: true });
      const other = path.join(home, "other");
      await (await import("node:fs/promises")).writeFile(other, "{}");
      await symlink(other, target);
      await expect(readDurableReceipt("session-3")).rejects.toThrow();
    } finally {
      setOracleHomeDirOverrideForTest(null);
    }
  });

  it("keeps auth on reconnecting events/status and preserves monotonic cursor", async () => {
    const seen: string[] = [];
    let events = 0;
    const server = http.createServer((req, res) => {
      seen.push(`${req.method} ${req.url} ${req.headers.authorization ?? ""}`);
      if (req.url?.startsWith("/v1/runs/r/events")) {
        events++;
        if (events === 1) {
          res.destroy();
          return;
        }
        res.end(JSON.stringify({ events: [{ seq: 0, event: { type: "accepted" } }] }));
        return;
      }
      if (req.url === "/v1/runs/r") {
        res.end(
          JSON.stringify({
            id: "r",
            state: "completed",
            phase: "terminal",
            queuePosition: 0,
            roughEtaMs: 100,
            requestHash: "a".repeat(64),
            createdAt: "2026-01-01T00:00:00.000Z",
            updatedAt: "2026-01-01T00:00:00.000Z",
            result: {
              answerText: "ok",
              answerMarkdown: "ok",
              tookMs: 1,
              answerTokens: 1,
              answerChars: 2,
            },
          }),
        );
        return;
      }
      if (req.url === "/v1/queue/status") {
        res.end(JSON.stringify({ active: 0, queued: 0, capacity: 1 }));
        return;
      }
      res.statusCode = 404;
      res.end();
    });
    await new Promise<void>((resolve) => server.listen(0, resolve));
    const port = (server.address() as any).port;
    try {
      const result = await watchDurableRemoteRun(`127.0.0.1:${port}`, "r", {
        token: "secret",
        reconnectDelayMs: 1,
        pollMs: 1,
        timeoutMs: 1000,
      });
      expect(result.snapshot.state).toBe("completed");
      expect(seen.some((x) => x.includes("Bearer secret"))).toBe(true);
      expect(await getDurableRemoteQueueStatus(`127.0.0.1:${port}`, "secret")).toMatchObject({
        active: 0,
      });
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  it("does not expose bearer tokens in malformed-response errors", async () => {
    const server = http.createServer((_req, res) => {
      res.end("Bearer secret-token");
    });
    await new Promise<void>((resolve) => server.listen(0, resolve));
    const port = (server.address() as any).port;
    try {
      await expect(
        getDurableRemoteQueueStatus(`127.0.0.1:${port}`, "secret-token"),
      ).rejects.toThrow(/malformed/);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  it("runs the executor through health, durable submit, watch, and terminal result", async () => {
    const calls: string[] = [];
    const server = http.createServer((req, res) => {
      calls.push(`${req.method} ${req.url}`);
      if (req.url === "/health")
        return void res.end(
          JSON.stringify({
            ok: true,
            version: "1",
            runtime: { name: "node", version: "25.1.0", major: 25, minimumMajor: 24 },
            capabilities: {
              schemaVersion: 1,
              features: [{ id: "oracle.remote.durable-queue", version: 1 }],
            },
          }),
        );
      if (req.method === "POST" && req.url === "/v1/runs")
        return void res.end(
          JSON.stringify({
            id: "r1",
            state: "queued",
            phase: "accepted",
            queuePosition: 0,
            roughEtaMs: 100,
            requestHash: "a".repeat(64),
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
          }),
        );
      if (req.url?.startsWith("/v1/runs/r1/events"))
        return void res.end(JSON.stringify({ events: [{ seq: 0, event: { type: "accepted" } }] }));
      if (req.url === "/v1/runs/r1")
        return void res.end(
          JSON.stringify({
            id: "r1",
            state: "completed",
            phase: "terminal",
            queuePosition: 0,
            roughEtaMs: 0,
            requestHash: "a".repeat(64),
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            result: {
              answerText: "ok",
              answerMarkdown: "ok",
              tookMs: 1,
              answerTokens: 1,
              answerChars: 2,
            },
          }),
        );
      res.statusCode = 404;
      res.end();
    });
    await new Promise<void>((resolve) => server.listen(0, resolve));
    const port = (server.address() as any).port;
    try {
      const result = await createRemoteBrowserExecutor({
        host: `127.0.0.1:${port}`,
        token: "secret",
      })({
        prompt: "hello",
        config: { timeoutMs: 4 * 60 * 60 * 1000 },
        sessionId: `test-${Date.now()}`,
      });
      expect(result.answerText).toBe("ok");
      expect(calls).toContain("POST /v1/runs");
      expect(calls.some((x) => x.includes("/v1/runs/r1/events"))).toBe(true);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  it("fails a missing capability before POSTing a run", async () => {
    let posts = 0;
    const server = http.createServer((req, res) => {
      if (req.url === "/health") {
        res.end(
          JSON.stringify({
            ok: true,
            version: "1",
            runtime: { name: "node", version: "25.1.0", major: 25, minimumMajor: 24 },
            capabilities: {
              schemaVersion: 1,
              features: [{ id: "oracle.remote.durable-queue", version: 1 }],
            },
          }),
        );
        return;
      }
      if (req.method === "POST") posts++;
      res.statusCode = 404;
      res.end();
    });
    await new Promise<void>((resolve) => server.listen(0, resolve));
    const port = (server.address() as any).port;
    try {
      await expect(
        createRemoteBrowserExecutor({
          host: `127.0.0.1:${port}`,
          requiredCapabilities: [{ id: "missing", version: 1 }],
        })({ prompt: "x", sessionId: `missing-${Date.now()}` }),
      ).rejects.toThrow(/required capability/);
      expect(posts).toBe(0);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  it("rejects unknown and nonmonotonic event records", async () => {
    const server = http.createServer((_req, res) => {
      res.end(
        JSON.stringify({
          events: [
            { seq: 0, event: { type: "accepted", extra: true } },
            { seq: 0, event: { type: "accepted" } },
          ],
        }),
      );
    });
    await new Promise<void>((resolve) => server.listen(0, resolve));
    const port = (server.address() as any).port;
    try {
      await expect(getDurableRemoteRunEvents(`127.0.0.1:${port}`, "r")).rejects.toThrow(
        /malformed or nonmonotonic/,
      );
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  it("reuses the idempotency key when the accepted POST response is destroyed", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-client-"));
    setOracleHomeDirOverrideForTest(home);
    let posts = 0;
    const keys: string[] = [];
    const { server, host } = await listen(async (req, res) => {
      if (req.url === "/health") return void res.end(JSON.stringify(health()));
      if (req.method === "POST" && req.url === "/v1/runs") {
        posts++;
        keys.push(String(req.headers["idempotency-key"]));
        await body(req);
        if (posts === 1) return void res.destroy();
        return void res.end(JSON.stringify(runSnapshot("same-run")));
      }
      if (req.url?.startsWith("/v1/runs/same-run/events"))
        return void res.end(JSON.stringify({ events: [] }));
      if (req.url === "/v1/runs/same-run")
        return void res.end(JSON.stringify(runSnapshot("same-run")));
      res.statusCode = 404;
      res.end();
    });
    try {
      const result = await createRemoteBrowserExecutor({ host })({
        prompt: "x",
        sessionId: "destroy-once",
      });
      expect(result.answerText).toBe("ok");
      expect(posts).toBe(2);
      expect(new Set(keys).size).toBe(1);
      expect((await readDurableReceipt("destroy-once"))?.runId).toBe("same-run");
    } finally {
      await close(server);
      setOracleHomeDirOverrideForTest(null);
    }
  });

  it("records unknown after two destroyed responses and recovers on the next invocation", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-client-"));
    setOracleHomeDirOverrideForTest(home);
    let posts = 0;
    let records = 0;
    const keys: string[] = [];
    const { server, host } = await listen(async (req, res) => {
      if (req.url === "/health") return void res.end(JSON.stringify(health()));
      if (req.method === "POST" && req.url === "/v1/runs") {
        posts++;
        keys.push(String(req.headers["idempotency-key"]));
        await body(req);
        if (posts <= 2) {
          records = 1;
          return void res.destroy();
        }
        return void res.end(JSON.stringify(runSnapshot("recovered")));
      }
      if (req.url?.startsWith("/v1/runs/recovered/events"))
        return void res.end(JSON.stringify({ events: [] }));
      if (req.url === "/v1/runs/recovered")
        return void res.end(JSON.stringify(runSnapshot("recovered")));
      res.statusCode = 404;
      res.end();
    });
    try {
      const executor = createRemoteBrowserExecutor({ host });
      await expect(executor({ prompt: "x", sessionId: "recover-me" })).rejects.toBeInstanceOf(
        DurableSubmissionUnknownError,
      );
      expect(await readDurableReceipt("recover-me")).toMatchObject({ submission: "unknown" });
      const result = await executor({ prompt: "x", sessionId: "recover-me" });
      expect(result.answerText).toBe("ok");
      expect(records).toBe(1);
      expect(posts).toBe(3);
      expect(new Set(keys).size).toBe(1);
    } finally {
      await close(server);
      setOracleHomeDirOverrideForTest(null);
    }
  });

  it("does not write an unknown receipt for a definite pre-submit refusal", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-client-"));
    setOracleHomeDirOverrideForTest(home);
    const sessionId = "refused-before-connect";
    try {
      await expect(
        createRemoteBrowserExecutor({ host: "127.0.0.1:1" })({ prompt: "x", sessionId }),
      ).rejects.toThrow(/Could not reach|ECONNREFUSED/);
      expect(await readDurableReceipt(sessionId)).toBeUndefined();
    } finally {
      setOracleHomeDirOverrideForTest(null);
    }
  });

  it("does not retry permanent POST responses", async () => {
    let posts = 0;
    const { server, host } = await listen(async (req, res) => {
      if (req.method === "POST") {
        posts++;
        await body(req);
        res.statusCode = 401;
        return void res.end(JSON.stringify({ error: "unauthorized" }));
      }
      if (req.url === "/health") return void res.end(JSON.stringify(health()));
      res.statusCode = 404;
      res.end();
    });
    try {
      await expect(
        submitDurableRemoteRun({
          host,
          idempotencyKey: "a".repeat(64),
          payload: { prompt: "", attachments: [], browserConfig: {}, options: {} },
        }),
      ).rejects.toThrow(/HTTP 401/);
      expect(posts).toBe(1);
    } finally {
      await close(server);
    }
  });

  it("strips capture-only prompt, attachments, fallback, followups, and model fields on the wire", async () => {
    let wire: any;
    const { server, host } = await listen(async (req, res) => {
      if (req.url === "/health") return void res.end(JSON.stringify(health()));
      if (req.method === "POST" && req.url === "/v1/runs") {
        wire = await body(req);
        return void res.end(JSON.stringify(runSnapshot("capture")));
      }
      if (req.url?.startsWith("/v1/runs/capture/events"))
        return void res.end(JSON.stringify({ events: [] }));
      if (req.url === "/v1/runs/capture")
        return void res.end(JSON.stringify(runSnapshot("capture")));
      res.statusCode = 404;
      res.end();
    });
    try {
      const result = await createRemoteBrowserExecutor({ host })({
        prompt: "secret prompt",
        attachments: [],
        fallbackSubmission: { prompt: "fallback", attachments: [] },
        followUpPrompts: ["followup"],
        config: {
          captureOnly: true,
          desiredModel: "private-model",
          modelStrategy: "select",
          thinkingTime: "heavy",
          researchMode: "deep",
        },
        sessionId: "capture-wire",
      });
      expect(result.answerText).toBe("ok");
      expect(wire).toMatchObject({ prompt: "", attachments: [], options: {} });
      expect(wire.fallbackSubmission).toBeUndefined();
      expect(wire.browserConfig).not.toHaveProperty("desiredModel");
      expect(wire.browserConfig).not.toHaveProperty("modelStrategy");
      expect(wire.browserConfig).not.toHaveProperty("thinkingTime");
      expect(wire.browserConfig).not.toHaveProperty("researchMode");
    } finally {
      await close(server);
      setOracleHomeDirOverrideForTest(null);
    }
  });
});
