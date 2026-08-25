import { afterEach, describe, expect, it } from "vitest";
import http from "node:http";
import { mkdtemp } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createRemoteServer } from "../../src/remote/server.js";

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
  });
});
