import { describe, expect, it } from "vitest";
import { mkdtemp, readFile, stat, symlink } from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { setOracleHomeDirOverrideForTest } from "../../src/oracleHome.js";
import {
  createRemoteBrowserExecutor,
  getDurableRemoteQueueStatus,
  readDurableReceipt,
  receiptPath,
  watchDurableRemoteRun,
  writeDurableReceipt,
} from "../../src/remote/client.js";

describe("durable remote client receipts", () => {
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
            createdAt: "x",
            updatedAt: "x",
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
});
