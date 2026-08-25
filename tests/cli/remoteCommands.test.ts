import { afterEach, describe, expect, it } from "vitest";
import { execFile } from "node:child_process";
import http from "node:http";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const CLI_ENTRY = path.join(process.cwd(), "bin", "oracle-cli.ts");

const completedSnapshot = {
  id: "run-1",
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
};

describe("durable remote CLI connection options", () => {
  let server: http.Server | undefined;

  afterEach(async () => {
    if (server?.listening)
      await new Promise<void>((resolve, reject) =>
        server?.close((error) => (error ? reject(error) : resolve())),
      );
    server = undefined;
  });

  it("carries explicit host and token through status, cancel, and watch", async () => {
    const requests: Array<{ method?: string; url?: string; authorization?: string }> = [];
    server = http.createServer((req, res) => {
      requests.push({
        method: req.method,
        url: req.url,
        authorization: req.headers.authorization,
      });
      res.setHeader("Content-Type", "application/json");
      if (req.url === "/v1/queue/status")
        return void res.end(JSON.stringify({ active: 0, queued: 0, capacity: 4, backlog: 8 }));
      if (req.url === "/v1/runs/run-1/events?after=-1")
        return void res.end(JSON.stringify({ events: [] }));
      if (req.url === "/v1/runs/run-1") return void res.end(JSON.stringify(completedSnapshot));
      if (req.url === "/v1/runs/run-1/cancel" && req.method === "POST")
        return void res.end(JSON.stringify(completedSnapshot));
      res.statusCode = 404;
      res.end(JSON.stringify({ error: "not_found" }));
    });
    await new Promise<void>((resolve) => server?.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("test server did not bind");
    const host = `127.0.0.1:${address.port}`;

    const commands = [
      ["remote", "status", "--remote-host", host, "--remote-token", "test-token", "--json"],
      [
        "remote",
        "cancel",
        "run-1",
        "--remote-host",
        host,
        "--remote-token",
        "test-token",
        "--json",
      ],
      [
        "remote",
        "watch",
        "run-1",
        "--remote-host",
        host,
        "--remote-token",
        "test-token",
        "--json",
        "--timeout",
        "1s",
      ],
    ];
    for (const command of commands) {
      const { stdout, stderr } = await execFileAsync(
        process.execPath,
        ["--import", "tsx", CLI_ENTRY, ...command],
        { timeout: 10_000 },
      );
      expect(stderr).not.toContain("Remote host is required");
      expect(() => JSON.parse(stdout)).not.toThrow();
    }

    expect(requests.map(({ method, url }) => `${method} ${url}`)).toEqual([
      "GET /v1/queue/status",
      "POST /v1/runs/run-1/cancel",
      "GET /v1/runs/run-1/events?after=-1",
      "GET /v1/runs/run-1",
    ]);
    expect(requests.every(({ authorization }) => authorization === "Bearer test-token")).toBe(true);
  });
});
