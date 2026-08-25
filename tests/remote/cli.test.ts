import { describe, expect, it } from "vitest";
import { spawn } from "node:child_process";
import { mkdtemp } from "node:fs/promises";
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
