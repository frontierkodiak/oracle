import { expect, test, vi } from "vitest";
import http from "node:http";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createRemoteServer } from "../../src/remote/server.js";
import { createRemoteBrowserExecutor } from "../../src/remote/client.js";

const token = "synthetic-admission-test";
const result = {
  answerText: "synthetic",
  answerMarkdown: "synthetic",
  tookMs: 1,
  answerTokens: 1,
  answerChars: 9,
};
const payload = JSON.stringify({ prompt: "synthetic", options: {}, browserConfig: {} });
const health = async (port: number) =>
  fetch(`http://127.0.0.1:${port}/health`, { headers: { Authorization: `Bearer ${token}` } }).then(
    (r) => r.json(),
  );

test("legacy default refuses a second caller with HTTP 409", async () => {
  let finish!: () => void;
  let started = false;
  const server = await createRemoteServer(
    { host: "127.0.0.1", port: 0, token, logger: () => {} },
    {
      runBrowser: async () => {
        started = true;
        await new Promise<void>((r) => {
          finish = r;
        });
        return result;
      },
    },
  );
  const first = fetch(`http://127.0.0.1:${server.port}/runs`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}` },
    body: payload,
  });
  try {
    await vi.waitFor(() => expect(started).toBe(true));
    const second = await fetch(`http://127.0.0.1:${server.port}/runs`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
      body: payload,
    });
    expect(second.status).toBe(409);
    expect(await second.json()).toEqual({ error: "busy" });
    expect(await health(server.port)).toMatchObject({
      admissionMode: "legacy",
      maxConcurrentRuns: 1,
      maxQueuedRuns: 0,
    });
  } finally {
    finish();
    await (await first).text();
    await server.close();
  }
});

test("simultaneously completed request bodies cannot exceed the queue bound", async () => {
  const finishes: (() => void)[] = [];
  const server = await createRemoteServer(
    { host: "127.0.0.1", port: 0, token, logger: () => {}, maxConcurrentRuns: 1, maxQueuedRuns: 1 },
    {
      runBrowser: async ({ signal }) => {
        await new Promise<void>((resolve) => {
          finishes.push(resolve);
          signal?.addEventListener("abort", () => resolve(), { once: true });
        });
        return result;
      },
    },
  );
  const codes: number[] = [];
  const pending: Promise<number>[] = [];
  const requests = Array.from({ length: 6 }, () => {
    let resolve!: (value: number) => void;
    let reject!: (reason: Error) => void;
    pending.push(
      new Promise<number>((yes, no) => {
        resolve = yes;
        reject = no;
      }),
    );
    const request = http.request(
      {
        host: "127.0.0.1",
        port: server.port,
        path: "/runs",
        method: "POST",
        headers: { Authorization: `Bearer ${token}`, "Content-Length": Buffer.byteLength(payload) },
      },
      (response) => {
        codes.push(response.statusCode ?? 0);
        response.resume();
        response.on("end", () => resolve(response.statusCode ?? 0));
      },
    );
    request.on("error", reject);
    request.write(payload.slice(0, 1));
    return request;
  });
  try {
    await new Promise((resolve) => setTimeout(resolve, 100));
    for (const request of requests) request.end(payload.slice(1));
    await vi.waitFor(() => expect(codes.filter((code) => code === 503)).toHaveLength(4));
    expect(await health(server.port)).toMatchObject({
      activeRuns: 1,
      queuedRuns: 1,
      maxQueuedRuns: 1,
    });
    finishes.shift()?.();
    await vi.waitFor(() => expect(finishes).toHaveLength(1));
    finishes.shift()?.();
    expect((await Promise.all(pending)).sort()).toEqual([200, 200, 503, 503, 503, 503]);
  } finally {
    for (const request of requests) request.destroy();
    for (const finish of finishes) finish();
    await Promise.allSettled(pending);
    await server.close();
  }
});

test("host configuration and environment resolve capacity with browser precedence", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "oracle-admission-config-"));
  vi.stubEnv("ORACLE_HOME_DIR", home);
  vi.stubEnv("ORACLE_BROWSER_MAX_CONCURRENT_TABS", "1");
  await fs.writeFile(
    path.join(home, "config.json"),
    JSON.stringify({ browser: { maxConcurrentTabs: 2 } }),
  );
  let observed: number | undefined;
  const server = await createRemoteServer(
    { host: "127.0.0.1", port: 0, token, logger: () => {}, maxConcurrentRuns: 4 },
    {
      runBrowser: async (options) => {
        observed = options.config?.maxConcurrentTabs;
        return result;
      },
    },
  );
  try {
    expect(await health(server.port)).toMatchObject({ maxConcurrentRuns: 2 });
    await createRemoteBrowserExecutor({ host: `127.0.0.1:${server.port}`, token })({
      prompt: "synthetic",
      config: {},
    });
    expect(observed).toBe(2);
  } finally {
    await server.close();
    vi.unstubAllEnvs();
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("explicit cancellation refuses an old host before starting a run", async () => {
  let posts = 0;
  const server = http.createServer((req, res) => {
    if (req.method === "POST") posts++;
    res.setHeader("Content-Type", "application/json");
    res.end(
      JSON.stringify({
        ok: true,
        capabilities: {
          artifactTransfer: true,
          artifactProtocolVersion: 1,
          maxArtifactBytes: 1000,
        },
      }),
    );
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const executor = createRemoteBrowserExecutor({
      host: `127.0.0.1:${(server.address() as { port: number }).port}`,
      token,
    });
    await expect(
      executor({ prompt: "synthetic", config: {}, signal: new AbortController().signal }),
    ).rejects.toThrow(/does not support run cancellation/);
    expect(posts).toBe(0);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
