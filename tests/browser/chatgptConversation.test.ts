import { describe, expect, it, vi, afterEach } from "vitest";
import { readFile, mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import vm from "node:vm";
import { webcrypto, createHash } from "node:crypto";
import {
  buildNormalizeAndDigestExpressionForTest,
  captureProviderNativeConversation,
  finalizeProviderNativeCapture,
} from "../../src/browser/chatgptConversation.js";

const math = String.raw`\(\mathcal{F}_s = \sum_{n=0}^\infty \tfrac{1}{2}\,\Gamma(n)\)`;
const fixture = (text = math) =>
  JSON.stringify({
    conversation_id: "fixture-0001",
    current_node: "answer",
    mapping: {
      root: { parent: null, message: null },
      old: {
        parent: "root",
        message: {
          id: "old-id",
          author: { role: "assistant" },
          content: { content_type: "text", parts: ["old answer"] },
        },
      },
      user: {
        parent: "old",
        message: {
          id: "user-id",
          author: { role: "user" },
          content: { content_type: "text", parts: ["echo me"] },
        },
      },
      answer: {
        parent: "user",
        message: {
          id: "answer-id",
          author: { role: "assistant" },
          content: { content_type: "text", parts: [text] },
        },
      },
      alternate: {
        parent: "user",
        message: {
          id: "alternate-id",
          author: { role: "assistant" },
          content: { content_type: "text", parts: ["alternate answer"] },
        },
      },
    },
  });

function runtime(
  options: {
    body?: string;
    secondBody?: string;
    httpStatus?: number;
    origin?: string;
    failSecond?: boolean;
    fetchError?: boolean;
  } = {},
) {
  let documents = 0;
  const timers: ReturnType<typeof setTimeout>[] = [];
  const fetch = vi.fn(async (url: string, init: RequestInit) => {
    expect(init.redirect).toBe("error");
    if (options.fetchError)
      throw new Error("Cookie: synthetic-secret; Authorization: Bearer synthetic-token");
    if (url === "/api/auth/session") return Response.json({ accessToken: "synthetic-token" });
    expect(init.headers).toMatchObject({ Authorization: "Bearer synthetic-token" });
    documents++;
    if (options.failSecond && documents === 2) return new Response("blocked", { status: 403 });
    return new Response(
      documents === 2
        ? (options.secondBody ?? options.body ?? fixture())
        : (options.body ?? fixture()),
      { status: options.httpStatus ?? 200, headers: { "content-type": "application/json" } },
    );
  });
  const context = vm.createContext({
    fetch,
    location: { origin: options.origin ?? "https://chatgpt.com" },
    AbortSignal,
    TextDecoder,
    TextEncoder,
    crypto: webcrypto,
    setTimeout: (fn: () => void, ms: number) => {
      const timer = setTimeout(fn, ms);
      timer.unref();
      timers.push(timer);
      return timer;
    },
  });
  const evaluate = vi.fn(async ({ expression }: { expression: string }) => ({
    result: { value: await vm.runInContext(expression, context) },
  }));
  return {
    Runtime: { evaluate } as never,
    fetch,
    get documents() {
      return documents;
    },
    close: () => timers.forEach(clearTimeout),
  };
}

afterEach(() => {
  vi.unstubAllEnvs();
  vi.useRealTimers();
});

describe("provider-native capture", () => {
  it("hashes known text fields from the contributor's recorded fixture without guessing mixed content", async () => {
    const raw = await readFile(
      new URL("../fixtures/provider-conversation.json", import.meta.url),
      "utf8",
    );
    const result = await vm.runInNewContext(buildNormalizeAndDigestExpressionForTest(raw), {
      crypto: webcrypto,
      TextEncoder,
    });
    expect(result.perTurn.map((turn: { sha256: string | null }) => turn.sha256)).toEqual([
      null,
      "0da6f512884d7b4f90e33fa87821c389f486064039183d835939ba805f52c9e1",
      "700f20751ff52acebe46193c1cc4d84488b4231fb72d00b5e0e595810e39fed3",
      "7cbbfb8e46caf3661d65b2c94dd78c20ec2e5a279aab31e641d903560262bfa9",
      null,
    ]);
  });

  it("preserves exact raw bytes and independently hashes the active branch", async () => {
    const raw = fixture();
    const other = raw.replace('"current_node"', '"volatile":1,"current_node"');
    const page = runtime({ body: raw, secondBody: other });
    try {
      const outcome = await captureProviderNativeConversation({
        ...page,
        conversationId: "fixture-0001",
      });
      expect(outcome.status).toBe("captured");
      if (outcome.status !== "captured") return;
      expect(page.documents).toBe(2);
      expect(outcome.capture.rawText).toBe(raw);
      expect(outcome.capture.rawSha256).toBe(createHash("sha256").update(raw).digest("hex"));
      expect(outcome.capture.documentHashesMatch).toBe(false);
      expect(outcome.capture.evidence?.perTurn.map((t) => t.messageId)).toEqual([
        "old-id",
        "user-id",
        "answer-id",
      ]);
      expect(outcome.capture.evidence?.perTurn.at(-1)?.sha256).toBe(
        createHash("sha256").update(math).digest("hex"),
      );
      expect(JSON.stringify(outcome)).not.toContain("synthetic-token");
    } finally {
      page.close();
    }
  });

  it.each([
    [math, "answer-id", "matched"],
    [` ${math}\n`, "answer-id", "matched"],
    [math.replace("_s", "*s"), "answer-id", "divergent"],
    ["echo me", "answer-id", "divergent"],
    ["old answer", "answer-id", "divergent"],
    ["alternate answer", "answer-id", "divergent"],
    ["echo me", "user-id", "unknown"],
    [math, undefined, "unknown"],
    [math, "missing-id", "unknown"],
  ])(
    "classifies answer %s bound to %s as %s",
    async (answerMarkdown, answerMessageId, expected) => {
      const page = runtime();
      try {
        const result = await finalizeProviderNativeCapture({
          ...page,
          conversationId: "fixture-0001",
          answerMarkdown,
          answerMessageId,
        });
        expect(result.summary.answerFidelity).toBe(expected);
        expect(result.summary.materializedToDisk).toBe(false);
      } finally {
        page.close();
      }
    },
  );

  it("writes private verbatim artifacts and truthful materialization evidence, even when fetch B fails", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "oracle-provider-"));
    vi.stubEnv("ORACLE_HOME_DIR", root);
    const page = runtime({ failSecond: true });
    try {
      const { summary, artifacts } = await finalizeProviderNativeCapture({
        ...page,
        conversationId: "fixture-0001",
        sessionId: "proof",
        answerMarkdown: math,
        answerMessageId: "answer-id",
      });
      expect(summary).toMatchObject({
        status: "captured",
        answerFidelity: "unknown",
        materializedToDisk: true,
        evidenceFailure: { reason: "challenged" },
      });
      expect(artifacts).toHaveLength(2);
      expect(await readFile(artifacts[0]!.path, "utf8")).toBe(fixture());
      const evidence = JSON.parse(await readFile(artifacts[1]!.path, "utf8"));
      expect(evidence.materializedToDisk).toBe(true);
      expect(evidence.materializedDocument.sha256).toBe(artifacts[0]!.sha256);
      if (process.platform !== "win32")
        expect((await stat(artifacts[0]!.path)).mode & 0o777).toBe(0o600);
    } finally {
      page.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  it.each([
    [{ httpStatus: 403 }, "challenged"],
    [{ httpStatus: 500 }, "http-error"],
    [{ body: "<html>challenge</html>" }, "invalid-document"],
    [{ body: "" }, "empty-document"],
    [{ body: "x".repeat(8 * 1024 * 1024 + 1) }, "document-too-large"],
    [{ origin: "https://untrusted.example" }, "wrong-origin"],
    [{ fetchError: true }, "evaluate-failed"],
  ])("fails closed for %j", async (options, reason) => {
    const page = runtime(options);
    const log = vi.fn();
    try {
      const result = await finalizeProviderNativeCapture({
        ...page,
        conversationId: "fixture-0001",
        logger: log,
      });
      expect(result.summary).toMatchObject({
        status: "unavailable",
        answerFidelity: "unknown",
        failure: { reason },
      });
      expect(JSON.stringify([result, log.mock.calls])).not.toMatch(
        /synthetic-secret|synthetic-token/,
      );
      if (reason === "wrong-origin") expect(page.fetch).not.toHaveBeenCalled();
    } finally {
      page.close();
    }
  });

  it.each([undefined, "../escape"])(
    "does not fetch without a safe conversation ID: %s",
    async (conversationId) => {
      const evaluate = vi.fn();
      const outcome = await captureProviderNativeConversation({
        Runtime: { evaluate } as never,
        conversationId,
      });
      expect(outcome.status).toBe("unavailable");
      expect(evaluate).not.toHaveBeenCalled();
    },
  );

  it("bounds a hung renderer and suppresses arbitrary CDP exception details", async () => {
    vi.useFakeTimers();
    const pending = captureProviderNativeConversation({
      Runtime: { evaluate: () => new Promise(() => {}) } as never,
      conversationId: "fixture-0001",
    });
    await vi.advanceTimersByTimeAsync(30_001);
    expect(await pending).toMatchObject({ status: "unavailable", failure: { reason: "timeout" } });
    const outcome = await captureProviderNativeConversation({
      Runtime: {
        evaluate: async () => ({
          exceptionDetails: { text: "Bearer synthetic-token" },
          result: {},
        }),
      } as never,
      conversationId: "fixture-0001",
    });
    expect(JSON.stringify(outcome)).not.toContain("synthetic-token");
  });

  it("reports filesystem failures without throwing or logging paths/secrets", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "oracle-provider-"));
    const file = path.join(root, "synthetic-secret");
    await writeFile(file, "not a directory");
    vi.stubEnv("ORACLE_HOME_DIR", file);
    const page = runtime();
    try {
      const result = await finalizeProviderNativeCapture({
        ...page,
        conversationId: "fixture-0001",
        sessionId: "proof",
        logger: () => {
          throw new Error("synthetic-secret");
        },
      });
      expect(result.summary.failure?.reason).toBe("write-failed");
      expect(result.summary.materializedToDisk).toBe(false);
      expect(JSON.stringify(result)).not.toContain("synthetic-secret");
    } finally {
      page.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  it("marks unknown/multimodal content unknown without reserializing its numbers", async () => {
    const data = JSON.parse(fixture());
    data.mapping.answer.message.content = {
      content_type: "future",
      payload: { tiny: 1e-7, huge: 1e20, negativeZero: -0 },
    };
    const result = await vm.runInNewContext(
      buildNormalizeAndDigestExpressionForTest(JSON.stringify(data)),
      { crypto: webcrypto, TextEncoder },
    );
    expect(result.perTurn.at(-1)).toMatchObject({
      sha256: null,
      bytes: null,
      contentType: "future",
    });
  });

  it.each(["missing", "cycle"])("rejects an ambiguous active branch: %s", async (kind) => {
    const data = JSON.parse(fixture());
    if (kind === "missing") delete data.current_node;
    else data.mapping.root.parent = "answer";
    const result = await vm.runInNewContext(
      buildNormalizeAndDigestExpressionForTest(JSON.stringify(data)),
      { crypto: webcrypto, TextEncoder },
    );
    expect(result).toMatchObject({ ok: false, reason: "invalid-document" });
  });
});
