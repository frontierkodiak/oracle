import { describe, expect, it } from "vitest";
import { spawn } from "node:child_process";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";

function runCli(
  args: string[],
  env: NodeJS.ProcessEnv,
): Promise<{ code: number; stdout: string; stderr: string }> {
  return new Promise((resolve) => {
    const entry = path.resolve("bin/oracle-cli.ts");
    const child = spawn(process.execPath, ["--no-deprecation", "--import", "tsx", entry, ...args], {
      env,
      cwd: process.cwd(),
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => (stdout += chunk));
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.on("close", (code) => resolve({ code: code ?? 1, stdout, stderr }));
  });
}

describe("remote submit CLI", () => {
  it("uses explicit prompt and browser flags from the merged Commander option view", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-cli-"));
    let payload: any;
    let authorization: string | undefined;
    let requestPath: string | undefined;
    const server = http.createServer(async (req, res) => {
      if (req.url === "/health") {
        res.setHeader("content-type", "application/json");
        res.end(
          JSON.stringify({
            ok: true,
            version: "test",
            runtime: { name: "node", version: "24.0.0", major: 24, minimumMajor: 24 },
            capabilities: {
              schemaVersion: 1,
              features: [
                {
                  id: "oracle.remote.durable-queue",
                  version: 1,
                  limits: { maxQueued: 10, maxConcurrentRuns: 2 },
                },
              ],
            },
          }),
        );
        return;
      }
      if (req.method !== "POST") {
        res.statusCode = 404;
        res.end();
        return;
      }
      authorization = req.headers.authorization;
      requestPath = req.url;
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(Buffer.from(chunk));
      payload = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      res.setHeader("content-type", "application/json");
      res.end(
        JSON.stringify({
          id: "run-cli-regression",
          state: "queued",
          phase: "accepted",
          queuePosition: 2,
          roughEtaMs: 900,
          requestHash: "a".repeat(64),
          createdAt: "2026-01-01T00:00:00.000Z",
          updatedAt: "2026-01-01T00:00:00.000Z",
        }),
      );
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const host = `127.0.0.1:${(server.address() as any).port}`;
    try {
      const result = await runCli(
        [
          "remote",
          "submit",
          "--prompt",
          "explicit prompt wins",
          "--model",
          "gpt-5.5-pro",
          "--browser-thinking-time",
          "pro",
          "--browser-timeout",
          "17s",
          "--browser-input-timeout",
          "8s",
          "--browser-attachment-timeout",
          "9s",
          "--browser-keep-browser",
          "--browser-model-strategy",
          "current",
          "--browser-research",
          "deep",
          "--verbose",
          "--chatgpt-url",
          "https://chatgpt.com/g/g-regression",
          "--session-id",
          "cli-regression-session",
          "--remote-host",
          host,
          "--remote-token",
          "cli-secret",
          "--json",
        ],
        {
          ...process.env,
          ORACLE_HOME_DIR: home,
          ORACLE_REMOTE_HOST: "127.0.0.1:1",
          ORACLE_REMOTE_TOKEN: "poisoned-env-token",
          ORACLE_MODEL: "poisoned-env-model",
        },
      );
      expect(result.code, result.stderr).toBe(0);
      expect(JSON.parse(result.stdout)).toMatchObject({ runId: "run-cli-regression" });
      expect(payload.prompt).toBe("explicit prompt wins");
      expect(authorization).toBe("Bearer cli-secret");
      expect(requestPath).toBe("/v1/runs");
      expect(payload.browserConfig.desiredModel).toBe("GPT-5.5");
      expect(payload.browserConfig.timeoutMs).toBe(17_000);
      expect(payload.browserConfig.inputTimeoutMs).toBe(8_000);
      expect(payload.browserConfig.attachmentTimeoutMs).toBe(9_000);
      expect(payload.browserConfig.keepBrowser).toBe(true);
      expect(payload.browserConfig.modelStrategy).toBe("current");
      expect(payload.browserConfig.thinkingTime).toBe("pro");
      expect(payload.browserConfig.researchMode).toBe("deep");
      expect(payload.options.verbose).toBe(true);
      expect(payload.browserConfig.url).toContain("chatgpt.com/g/g-regression");
      expect(payload.options.sessionId).toBe("cli-regression-session");
      expect(payload.prompt).not.toContain("poisoned");
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});

describe("remote recover CLI", () => {
  const snapshot = (id: string) => ({
    id,
    state: "completed",
    phase: "terminal",
    queuePosition: 0,
    roughEtaMs: 0,
    requestHash: "a".repeat(64),
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:01.000Z",
    result: {
      answerText: "ok",
      answerMarkdown: "ok",
      tookMs: 1,
      answerTokens: 1,
      answerChars: 2,
    },
  });

  const healthEnvelope = {
    ok: true,
    version: "test",
    runtime: { name: "node", version: "24.0.0", major: 24, minimumMajor: 24 },
    queueId: "queue-cli",
    capabilities: {
      schemaVersion: 1,
      features: [
        {
          id: "oracle.remote.durable-queue",
          version: 1,
          limits: { maxQueued: 8, maxConcurrentRuns: 4 },
        },
        { id: "oracle.remote.idempotency-lookup", version: 1 },
      ],
    },
  };

  async function writeReceipt(home: string, sessionId: string, body: Record<string, unknown>) {
    const dir = path.join(home, "sessions", sessionId);
    await mkdir(dir, { recursive: true, mode: 0o700 });
    await writeFile(path.join(dir, "durable-queue.json"), JSON.stringify(body), { mode: 0o600 });
  }

  it("resolves a receipt read-only and distinguishes miss, lost run, and unverified identity", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-cli-recover-"));
    const foundSession = "cli-recover-found";
    const missingSession = "cli-recover-missing";
    const lostRunSession = "cli-recover-lost";
    const unverifiedSession = "cli-recover-unverified";
    const foundKey = "a".repeat(64);
    await writeReceipt(home, foundSession, {
      sessionId: foundSession,
      idempotencyKey: foundKey,
      submission: "unknown",
      queueId: "queue-cli",
    });
    await writeReceipt(home, missingSession, {
      sessionId: missingSession,
      idempotencyKey: "b".repeat(64),
      submission: "unknown",
      queueId: "queue-cli",
    });
    await writeReceipt(home, lostRunSession, {
      sessionId: lostRunSession,
      idempotencyKey: "c".repeat(64),
      runId: "ghost-run",
    });
    await writeReceipt(home, unverifiedSession, {
      sessionId: unverifiedSession,
      idempotencyKey: "d".repeat(64),
      submission: "unknown",
    });
    let posts = 0;
    const server = http.createServer((req, res) => {
      res.setHeader("content-type", "application/json");
      if (req.url === "/health") {
        res.end(JSON.stringify(healthEnvelope));
        return;
      }
      if (req.method === "GET" && req.url === `/v1/runs/by-idempotency-key/${foundKey}`) {
        res.end(JSON.stringify(snapshot("recovered-run")));
        return;
      }
      if (req.method === "POST") {
        posts += 1;
        res.end();
        return;
      }
      res.statusCode = 404;
      res.end(JSON.stringify({ error: "run_not_found" }));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const host = `127.0.0.1:${(server.address() as any).port}`;
    const recover = (sessionId: string) =>
      runCli(["remote", "recover", "--session-id", sessionId, "--remote-host", host, "--json"], {
        ...process.env,
        ORACLE_HOME_DIR: home,
      });
    try {
      const found = await recover(foundSession);
      expect(found.code, found.stderr).toBe(0);
      expect(JSON.parse(found.stdout)).toMatchObject({ found: true, runId: "recovered-run" });

      const missing = await recover(missingSession);
      expect(missing.code).toBe(2);
      expect(JSON.parse(missing.stdout)).toMatchObject({ found: false, reason: "not_found" });

      const lost = await recover(lostRunSession);
      expect(lost.code).toBe(3);
      expect(JSON.parse(lost.stdout)).toMatchObject({
        found: false,
        reason: "missing_run",
        runId: "ghost-run",
      });
      expect(lost.stdout).not.toContain("No durable run was accepted");

      const unverified = await recover(unverifiedSession);
      expect(unverified.code).toBe(7);
      expect(JSON.parse(unverified.stdout)).toMatchObject({
        found: false,
        reason: "identity_unverified",
      });

      expect(posts).toBe(0);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});
