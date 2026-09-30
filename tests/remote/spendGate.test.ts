import { afterEach, describe, expect, it } from "vitest";
import http from "node:http";
import { mkdtemp } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createRemoteServer } from "../../src/remote/server.js";
import {
  createCommandSpendGate,
  SpendGateRefusedError,
  type SpendGateSubmission,
} from "../../src/remote/spendGate.js";
import { createSendGate } from "../../src/browser/sendGate.js";
import type { BrowserRunOptions } from "../../src/browser/types.js";

function call(port: number, method: string, route: string, body?: unknown, key?: string) {
  return new Promise<{ status: number; json: any }>((resolve, reject) => {
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

async function settled(port: number, id: string) {
  for (let i = 0; i < 200; i++) {
    const run = await call(port, "GET", `/v1/runs/${id}`);
    if (["completed", "failed", "canceled", "unknown"].includes(run.json.state)) return run.json;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("run did not settle");
}

/** A mocked browser that sends only what the gate lets through, like the real pre-click fence. */
function fencedBrowser(sent: number[]) {
  return async (options: BrowserRunOptions) => {
    const sends = 1 + (options.followUpPrompts?.length ?? 0);
    for (let ordinal = 0; ordinal < sends; ordinal++) {
      await options.beforeSend?.({ ordinal, attempt: 0 });
      sent.push(ordinal);
    }
    return {
      answerText: "ok",
      answerMarkdown: "ok",
      tookMs: 1,
      answerTokens: 1,
      answerChars: 2,
      promptSubmitted: true,
    };
  };
}

const run = (followUpPrompts?: string[]) => ({
  prompt: "prove it",
  attachments: [],
  browserConfig: { desiredModel: "Latest", thinkingTime: "pro" },
  options: { sessionId: "research-one", ...(followUpPrompts ? { followUpPrompts } : {}) },
});

describe("browser send gate", () => {
  it("reserves every attempt, recovery retries included, before it is marked", async () => {
    const events: string[] = [];
    const gate = createSendGate(async ({ ordinal, attempt }) => {
      events.push(`reserve ${ordinal}.${attempt}`);
    });
    const mark = async () => {
      events.push("mark");
    };
    await gate.beforeAttempt(mark);
    await gate.beforeAttempt(mark); // a recovery retry may dispatch again
    gate.setOrdinal(1);
    await gate.beforeAttempt(mark);
    expect(events).toEqual(["reserve 0.0", "mark", "reserve 0.1", "mark", "reserve 1.0", "mark"]);
  });

  it("a refused reservation stops the send before it is marked", async () => {
    const events: string[] = [];
    const gate = createSendGate(async () => {
      throw new SpendGateRefusedError("cap");
    });
    await expect(gate.beforeAttempt(async () => void events.push("mark"))).rejects.toThrow("cap");
    expect(events).toEqual([]);
  });
});

describe("command spend gate", () => {
  it("asks the ledger about this exact send and refuses on any failure", async () => {
    const calls: string[][] = [];
    const ok = createCommandSpendGate({
      command: "/ledger",
      oracleHome: "/home/.oracle",
      exec: (_file, args, _opts, cb) => {
        calls.push(args);
        cb(null, "{}", "");
      },
    });
    await ok({
      runId: "run-1",
      sessionId: "s",
      ordinal: 2,
      attempt: 1,
      model: "Latest",
      effort: "pro",
    });
    expect(calls[0]).toEqual([
      "charge",
      "--at-submit",
      "--route",
      "oracle-serve",
      "--session",
      "s",
      "--oracle-home",
      "/home/.oracle",
      "--run-id",
      "run-1",
      "--ordinal",
      "2",
      "--attempt",
      "1",
      "--model",
      "Latest",
      "--effort",
      "pro",
    ]);
    const refused = createCommandSpendGate({
      command: "/ledger",
      oracleHome: "/h",
      exec: (_file, _args, _opts, cb) => cb(new Error("exit 2"), "", "cap would be exceeded"),
    });
    await expect(refused({ runId: "r", ordinal: 0, attempt: 0 })).rejects.toThrow(
      /cap would be exceeded/,
    );
    const missing = createCommandSpendGate({ command: "/nonexistent/pro-lane", oracleHome: "/h" });
    await expect(missing({ runId: "r", ordinal: 0, attempt: 0 })).rejects.toBeInstanceOf(
      SpendGateRefusedError,
    );
  });
});

describe("bridge spend gate", () => {
  let server: Awaited<ReturnType<typeof createRemoteServer>> | undefined;
  afterEach(async () => {
    await server?.close();
    server = undefined;
  });

  const start = async (options: Record<string, unknown>, deps: Record<string, unknown>) => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-spend-gate-"));
    server = await createRemoteServer(
      {
        host: "127.0.0.1",
        port: 0,
        token: "test",
        logger: () => {},
        queueHomeDir: home,
        ...options,
      },
      deps as any,
    );
    return server;
  };

  it("reserves every send of a run, follow-ups included, with the server's own run identity", async () => {
    const sent: number[] = [];
    const asked: SpendGateSubmission[] = [];
    const s = await start(
      {},
      {
        runBrowser: fencedBrowser(sent),
        spendGate: async (x: SpendGateSubmission) => void asked.push(x),
      },
    );
    const accepted = await call(s.port, "POST", "/v1/runs", run(["and then?"]), "k1");
    const done = await settled(s.port, accepted.json.id);
    expect(done.state).toBe("completed");
    expect(sent).toEqual([0, 1]);
    expect(asked.map((a) => [a.runId, a.sessionId, a.ordinal, a.model, a.effort])).toEqual([
      [accepted.json.id, "research-one", 0, "Latest", "pro"],
      [accepted.json.id, "research-one", 1, "Latest", "pro"],
    ]);
  });

  it("a refusal fails the run before anything is sent", async () => {
    const sent: number[] = [];
    const s = await start(
      {},
      {
        runBrowser: fencedBrowser(sent),
        spendGate: async () => {
          throw new SpendGateRefusedError("spend gate refused the send: cap would be exceeded");
        },
      },
    );
    const accepted = await call(s.port, "POST", "/v1/runs", run(), "k2");
    const done = await settled(s.port, accepted.json.id);
    expect(done.state).toBe("failed");
    expect(sent).toEqual([]);
  });

  it("a required gate with no command refuses every send", async () => {
    const sent: number[] = [];
    const s = await start({ spendGateRequired: true }, { runBrowser: fencedBrowser(sent) });
    const accepted = await call(s.port, "POST", "/v1/runs", run(), "k3");
    expect((await settled(s.port, accepted.json.id)).state).toBe("failed");
    expect(sent).toEqual([]);
  });

  it("a missing ledger command refuses every send", async () => {
    const sent: number[] = [];
    const s = await start(
      { spendGateCommand: "/nonexistent/pro-lane" },
      { runBrowser: fencedBrowser(sent) },
    );
    const accepted = await call(s.port, "POST", "/v1/runs", run(), "k4");
    expect((await settled(s.port, accepted.json.id)).state).toBe("failed");
    expect(sent).toEqual([]);
  });
});
