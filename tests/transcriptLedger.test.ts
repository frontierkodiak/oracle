import { describe, expect, it } from "vitest";
import { chmod, mkdir, readFile, rm, symlink, writeFile, lstat } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import { spawn } from "node:child_process";
import path from "node:path";
import { pathToFileURL } from "node:url";
import os from "node:os";
import { mkdtemp } from "node:fs/promises";
import { createHash } from "node:crypto";
import {
  TranscriptLedger,
  buildCaptureOnlySyncRequest,
  classifyObservationFailure,
  deriveChatgptProfileId,
  ingestProviderNativeArtifacts,
  LedgerArtifactPairError,
  parsePositiveFiniteInterval,
  selectSyncWatches,
} from "../src/transcriptLedger.js";
import { setOracleHomeDirOverrideForTest } from "../src/oracleHome.js";

async function fixture(dir: string, suffix: string, body = "hello") {
  const raw = {
    conversation_id: "conv-1",
    current_node: "a2",
    create_time: Date.now(),
    mapping: {
      root: { id: "root", parent: null, children: ["a1"], message: null },
      a1: {
        id: "a1",
        parent: "root",
        children: ["a2"],
        message: {
          author: { role: "user" },
          content: { content_type: "text", parts: ["question"] },
        },
      },
      a2: {
        id: "a2",
        parent: "a1",
        children: [],
        message: {
          author: { role: "assistant" },
          content: { content_type: "text", parts: [body] },
        },
      },
    },
  };
  const rawPath = path.join(dir, `${suffix}-raw.json`);
  const evidencePath = path.join(dir, `${suffix}-evidence.json`);
  const independentPath = path.join(dir, `${suffix}-independent.json`);
  const rawBytes = Buffer.from(JSON.stringify(raw));
  const digest = (value: string) => createHash("sha256").update(value).digest("hex");
  const decimal = (value: string) => [...Buffer.from(value, "hex")];
  const evidence = {
    schema: "oracle.provider-native-capture-evidence/v1",
    conversation_id: "conv-1",
    materialized_document: {
      sha256: createHash("sha256").update(rawBytes).digest("hex"),
      bytes: rawBytes.byteLength,
    },
    independent_document: {
      sha256: createHash("sha256").update(rawBytes).digest("hex"),
      bytes: rawBytes.byteLength,
    },
    independent_fetch: {
      document_sha256_decimal_bytes: decimal(
        createHash("sha256").update(rawBytes).digest("hex"),
      ).join(" "),
      document_bytes: rawBytes.byteLength,
      fetched_at: "2026-01-01T00:00:00.000Z",
    },
    per_turn: [
      {
        i: 0,
        role: "user",
        ct: "text",
        blen: Buffer.byteLength("question"),
        sha256_hex: digest("question"),
        sha256_dec: decimal(digest("question")).join(" "),
        attachments: [],
      },
      {
        i: 1,
        role: "assistant",
        ct: "text",
        blen: Buffer.byteLength(body),
        sha256_hex: digest(body),
        sha256_dec: decimal(digest(body)).join(" "),
        attachments: [],
      },
    ],
  };
  await writeFile(rawPath, rawBytes);
  await writeFile(evidencePath, JSON.stringify(evidence));
  await writeFile(independentPath, rawBytes);
  return { rawPath, evidencePath, independentPath, rawBytes };
}

async function tempRoot() {
  return mkdtemp(path.join(os.tmpdir(), "oracle-ledger-test-"));
}

describe("TranscriptLedger", () => {
  it("preserves authoritative raw bytes in immutable content-addressed objects", async () => {
    const dir = await tempRoot();
    const files = await fixture(dir, "one");
    const ledger = await TranscriptLedger.open({ root: path.join(dir, "ledger") });
    const result = await ledger.ingestPair({
      provider: "chatgpt",
      profileId: "profile-a",
      rawPath: files.rawPath,
      evidencePath: files.evidencePath,
      independentPath: files.independentPath,
    });
    ledger.close();
    const digest = createHash("sha256").update(files.rawBytes).digest("hex");
    expect(result.rawSha256).toBe(digest);
    expect(result.independentSha256).toBe(digest);
    expect(
      await readFile(path.join(dir, "ledger", "objects", "sha256", digest.slice(0, 2), digest)),
    ).toEqual(files.rawBytes);
    expect((await lstat(path.join(dir, "ledger", "index.sqlite"))).mode & 0o777).toBe(0o600);
  });

  it("deduplicates logical revisions while retaining each volatile raw observation", async () => {
    const dir = await tempRoot();
    const first = await fixture(dir, "one");
    const second = await fixture(dir, "two");
    const ledger = await TranscriptLedger.open({ root: path.join(dir, "ledger") });
    const a = await ledger.ingestPair({
      provider: "chatgpt",
      profileId: "profile-a",
      rawPath: first.rawPath,
      evidencePath: first.evidencePath,
      independentPath: first.independentPath,
    });
    const b = await ledger.ingestPair({
      provider: "chatgpt",
      profileId: "profile-a",
      rawPath: second.rawPath,
      evidencePath: second.evidencePath,
      independentPath: second.independentPath,
    });
    expect(a.normalizedSequenceSha256).toBe(b.normalizedSequenceSha256);
    expect(b.deduplicated).toBe(true);
    expect(ledger.list()[0]?.revisionCount).toBe(1);
    expect(ledger.list()[0]?.observationCount).toBe(2);
    ledger.close();
  });

  it("creates a distinct immutable revision when a selected-branch body changes", async () => {
    const dir = await tempRoot();
    const one = await fixture(dir, "one", "hello");
    const two = await fixture(dir, "two", "goodbye");
    const ledger = await TranscriptLedger.open({ root: path.join(dir, "ledger") });
    const a = await ledger.ingestPair({
      provider: "chatgpt",
      profileId: "profile-a",
      rawPath: one.rawPath,
      evidencePath: one.evidencePath,
      independentPath: one.independentPath,
    });
    const b = await ledger.ingestPair({
      provider: "chatgpt",
      profileId: "profile-a",
      rawPath: two.rawPath,
      evidencePath: two.evidencePath,
      independentPath: two.independentPath,
    });
    expect(a.revisionId).not.toBe(b.revisionId);
    expect(ledger.list()[0]?.revisionCount).toBe(2);
    expect(ledger.getRevisionTurns(b.revisionId)[1]?.bodyBytes).toBe(7);
    ledger.close();
  });

  it("records failed observations without representing them as deletion", async () => {
    const dir = await tempRoot();
    const ledger = await TranscriptLedger.open({ root: path.join(dir, "ledger") });
    const failed = ledger.recordFailedObservation({
      provider: "chatgpt",
      profileId: "profile-a",
      conversationId: "conv-1",
      status: "auth-unavailable",
      errorCode: "auth-unavailable",
    });
    expect(failed.observationId).toBeTruthy();
    expect(ledger.list()[0]).toMatchObject({
      state: "auth-unavailable",
      revisionCount: 0,
      observationCount: 1,
    });
    ledger.close();
  });

  it("rejects unsafe symlinked descendants and does not publish malformed pairs", async () => {
    const dir = await tempRoot();
    const root = path.join(dir, "ledger");
    await mkdir(root, { mode: 0o700 });
    await symlink(dir, path.join(root, "objects"));
    await expect(TranscriptLedger.open({ root })).rejects.toThrow(/symlinks/);

    const safeRoot = path.join(dir, "safe");
    const files = await fixture(dir, "bad");
    await writeFile(files.evidencePath, "not-json");
    const ledger = await TranscriptLedger.open({ root: safeRoot });
    await expect(
      ledger.ingestPair({
        provider: "chatgpt",
        profileId: "profile-a",
        rawPath: files.rawPath,
        evidencePath: files.evidencePath,
        independentPath: files.independentPath,
      }),
    ).rejects.toThrow();
    expect(ledger.list()).toEqual([]);
    ledger.close();
  });

  it("keeps ledger-owned directories private", async () => {
    const dir = await tempRoot();
    const root = path.join(dir, "ledger");
    const ledger = await TranscriptLedger.open({ root });
    ledger.close();
    for (const owned of [root, path.join(root, "objects"), path.join(root, "objects", "sha256")]) {
      expect((await lstat(owned)).mode & 0o777).toBe(0o700);
    }
  });

  it("builds sync requests with an empty prompt and capture-only fences", () => {
    const request = buildCaptureOnlySyncRequest("https://chatgpt.com/c/conv-1", "/profiles/oracle");
    expect(request.prompt).toBe("");
    expect(request.config).toMatchObject({
      captureOnly: true,
      captureProviderNative: true,
      manualLogin: true,
    });
    expect(request.config).not.toHaveProperty("followUpPrompts");
  });

  it("strictly rejects malformed graph and evidence correspondence", async () => {
    const dir = await tempRoot();
    const files = await fixture(dir, "strict");
    const raw = JSON.parse(await readFile(files.rawPath, "utf8")) as Record<string, any>;
    raw.mapping.a2.parent = "missing";
    await writeFile(files.rawPath, JSON.stringify(raw));
    const ledger = await TranscriptLedger.open({ root: path.join(dir, "ledger") });
    await expect(
      ledger.ingestPair({
        provider: "chatgpt",
        profileId: "profile-a",
        rawPath: files.rawPath,
        evidencePath: files.evidencePath,
        independentPath: files.independentPath,
      }),
    ).rejects.toThrow(/parent is invalid|parent\/child mismatch/);
    ledger.close();

    const second = await fixture(dir, "evidence");
    const evidence = JSON.parse(await readFile(second.evidencePath, "utf8")) as Record<string, any>;
    evidence.per_turn[1].sha256_hex = "0".repeat(64);
    await writeFile(second.evidencePath, JSON.stringify(evidence));
    const secondLedger = await TranscriptLedger.open({ root: path.join(dir, "ledger-two") });
    await expect(
      secondLedger.ingestPair({
        provider: "chatgpt",
        profileId: "profile-a",
        rawPath: second.rawPath,
        evidencePath: second.evidencePath,
        independentPath: second.independentPath,
      }),
    ).rejects.toThrow(/body hash mismatch/);
    secondLedger.close();
  });

  it("recovers unreferenced crash orphans and rejects oversized JSON depth", async () => {
    const dir = await tempRoot();
    const root = path.join(dir, "ledger");
    const ledger = await TranscriptLedger.open({ root });
    ledger.close();
    const orphan = Buffer.from("orphan");
    const digest = createHash("sha256").update(orphan).digest("hex");
    const objectDir = path.join(root, "objects", "sha256", digest.slice(0, 2));
    await mkdir(objectDir, { recursive: true, mode: 0o700 });
    await writeFile(path.join(objectDir, digest), orphan, { mode: 0o600 });
    const reopened = await TranscriptLedger.open({ root });
    expect(await lstat(path.join(objectDir, digest)).catch(() => null)).toBeNull();
    reopened.close();

    const legacyState = path.join(root, "state");
    await mkdir(legacyState, { mode: 0o700 });
    const legacyLock = path.join(legacyState, "publication.lock");
    await writeFile(
      legacyLock,
      JSON.stringify({ pid: Number.MAX_SAFE_INTEGER, token: "crashed", startedAt: 0 }),
      { mode: 0o600 },
    );
    const recoveredAfterCrash = await TranscriptLedger.open({ root });
    expect(await lstat(legacyLock)).toBeTruthy();
    recoveredAfterCrash.close();
    await rm(legacyLock);

    const files = await fixture(dir, "deep");
    const raw = JSON.parse(await readFile(files.rawPath, "utf8")) as Record<string, any>;
    let nested: Record<string, unknown> = {};
    for (let i = 0; i < 140; i += 1) nested = { nested };
    raw.extra = nested;
    await writeFile(files.rawPath, JSON.stringify(raw));
    const deepLedger = await TranscriptLedger.open({ root: path.join(dir, "deep-ledger") });
    await expect(
      deepLedger.ingestPair({
        provider: "chatgpt",
        profileId: "profile-a",
        rawPath: files.rawPath,
        evidencePath: files.evidencePath,
        independentPath: files.independentPath,
      }),
    ).rejects.toThrow(/depth bounds/);
    deepLedger.close();
  });

  it("keeps default profile identity stable across cwd changes", () => {
    setOracleHomeDirOverrideForTest("/tmp/oracle-home-stable");
    const before = process.cwd();
    try {
      process.chdir("/");
      const first = deriveChatgptProfileId({ chromeProfile: "Default" });
      process.chdir("/tmp");
      const second = deriveChatgptProfileId({ chromeProfile: "Default" });
      expect(first).toBe(second);
    } finally {
      process.chdir(before);
      setOracleHomeDirOverrideForTest(null);
    }
  });

  it("requires explicit sync scope, honors enabled, and preserves failed watch success time", async () => {
    expect(() => selectSyncWatches([], undefined, false)).toThrow(/requires/);
    expect(
      selectSyncWatches(
        [
          { conversationId: "enabled", enabled: 1 },
          { conversationId: "disabled", enabled: 0 },
        ],
        undefined,
        true,
      ).map((row) => row.conversationId),
    ).toEqual(["enabled"]);
    expect(parsePositiveFiniteInterval("0.5")).toBe(0.5);
    expect(() => parsePositiveFiniteInterval("0")).toThrow(/positive/);
    expect(() => parsePositiveFiniteInterval("Infinity")).toThrow(/positive/);
    expect(classifyObservationFailure({ details: { failure: { reason: "challenged" } } })).toBe(
      "challenged",
    );
    expect(
      classifyObservationFailure({ details: { failure: { reason: "auth-session-unavailable" } } }),
    ).toBe("auth-unavailable");

    const dir = await tempRoot();
    const ledger = await TranscriptLedger.open({ root: path.join(dir, "ledger") });
    const watch = ledger.watch({ provider: "chatgpt", profileId: "p", conversationId: "c" });
    ledger.recordWatchAttempt(watch.watchId, { observationId: "success" });
    const successAt = ledger.watchRows()[0]?.lastSuccessAt;
    ledger.recordWatchAttempt(watch.watchId, {
      observationId: "failure",
      errorCode: "capture-failed",
    });
    expect(ledger.watchRows()[0]?.lastSuccessAt).toBe(successAt);
    ledger.close();
  });

  it("emits a typed warning for an incomplete provider artifact pair", async () => {
    await expect(
      ingestProviderNativeArtifacts({
        profileId: "p",
        artifacts: [{ path: "/tmp/raw", label: "provider-native-conversation-raw" }],
      }),
    ).rejects.toBeInstanceOf(LedgerArtifactPairError);
  });

  it("rejects independent evidence placeholders and disconnected multi-root forests", async () => {
    const dir = await tempRoot();
    const files = await fixture(dir, "independent");
    const evidence = JSON.parse(await readFile(files.evidencePath, "utf8")) as Record<string, any>;
    const ledger = await TranscriptLedger.open({ root: path.join(dir, "ledger") });
    evidence.independent_fetch = {};
    await writeFile(files.evidencePath, JSON.stringify(evidence));
    await expect(
      ledger.ingestPair({
        provider: "chatgpt",
        profileId: "p",
        rawPath: files.rawPath,
        evidencePath: files.evidencePath,
        independentPath: files.independentPath,
      }),
    ).rejects.toThrow(/unsupported shape/);
    evidence.independent_fetch = {
      document_sha256_decimal_bytes: "1 2 3",
      document_bytes: 3,
      fetched_at: "2026-01-01T00:00:00.000Z",
    };
    await writeFile(files.evidencePath, JSON.stringify(evidence));
    await expect(
      ledger.ingestPair({
        provider: "chatgpt",
        profileId: "p",
        rawPath: files.rawPath,
        evidencePath: files.evidencePath,
        independentPath: files.independentPath,
      }),
    ).rejects.toThrow(/exactly 32 decimal bytes/);
    const forged = JSON.parse(await readFile(files.independentPath, "utf8")) as Record<string, any>;
    forged.mapping.a2.message.content.parts = ["forged independent body"];
    const forgedBytes = Buffer.from(JSON.stringify(forged));
    const forgedSha256 = createHash("sha256").update(forgedBytes).digest("hex");
    evidence.independent_document = { sha256: forgedSha256, bytes: forgedBytes.byteLength };
    evidence.independent_fetch = {
      document_sha256_decimal_bytes: [...Buffer.from(forgedSha256, "hex")].join(" "),
      document_bytes: forgedBytes.byteLength,
      fetched_at: "2026-01-01T00:00:00.000Z",
    };
    await writeFile(files.independentPath, forgedBytes);
    await writeFile(files.evidencePath, JSON.stringify(evidence));
    await expect(
      ledger.ingestPair({
        provider: "chatgpt",
        profileId: "p",
        rawPath: files.rawPath,
        evidencePath: files.evidencePath,
        independentPath: files.independentPath,
      }),
    ).rejects.toThrow(/does not correspond|selected branch differs/);
    ledger.close();

    const raw = JSON.parse(await readFile(files.rawPath, "utf8")) as Record<string, any>;
    raw.mapping.other = { id: "other", parent: null, children: [], message: null };
    await writeFile(files.rawPath, JSON.stringify(raw));
    const forestLedger = await TranscriptLedger.open({ root: path.join(dir, "forest-ledger") });
    await expect(
      forestLedger.ingestPair({
        provider: "chatgpt",
        profileId: "p",
        rawPath: files.rawPath,
        evidencePath: files.evidencePath,
        independentPath: files.independentPath,
      }),
    ).rejects.toThrow(/exactly one root/);
    forestLedger.close();
  });

  it("ingests the canonical all-content-types fixture and fails closed on evidence tampering", async () => {
    const dir = await tempRoot();
    const fixturePath = path.join(
      process.cwd(),
      "tests/fixtures/provider-conversation-normalization.json",
    );
    const source = JSON.parse(await readFile(fixturePath, "utf8")) as {
      raw: string;
      expected: Array<{ role: string; content_type: string; bytes: number; sha256: string }>;
    };
    const rawBytes = Buffer.from(source.raw, "utf8");
    const rawPath = path.join(dir, "raw.json");
    const evidencePath = path.join(dir, "evidence.json");
    const independentPath = path.join(dir, "independent.json");
    const decimal = (hex: string) => [...Buffer.from(hex, "hex")].join(" ");
    const evidence = {
      schema: "oracle.provider-native-capture-evidence/v1",
      conversation_id: "fixture-0001",
      materialized_document: {
        sha256: createHash("sha256").update(rawBytes).digest("hex"),
        bytes: rawBytes.byteLength,
      },
      independent_document: {
        sha256: createHash("sha256").update(rawBytes).digest("hex"),
        bytes: rawBytes.byteLength,
      },
      independent_fetch: {
        document_sha256_decimal_bytes: decimal(createHash("sha256").update(rawBytes).digest("hex")),
        document_bytes: rawBytes.byteLength,
        fetched_at: "2026-01-01T00:00:00.000Z",
      },
      per_turn: source.expected.map((turn, index) => ({
        i: index,
        role: turn.role,
        ct: turn.content_type,
        blen: turn.bytes,
        sha256_hex: turn.sha256,
        sha256_dec: decimal(turn.sha256),
        attachments: [],
      })),
    };
    await writeFile(rawPath, rawBytes, { mode: 0o600 });
    await writeFile(independentPath, rawBytes, { mode: 0o600 });
    await writeFile(evidencePath, JSON.stringify(evidence), { mode: 0o600 });
    const ledger = await TranscriptLedger.open({ root: path.join(dir, "ledger") });
    const result = await ledger.ingestPair({
      provider: "chatgpt",
      profileId: "fixture-profile",
      rawPath,
      evidencePath,
      independentPath,
    });
    expect(
      ledger.getRevisionTurns(result.revisionId).map((turn) => [turn.contentType, turn.bodyBytes]),
    ).toEqual(source.expected.map((turn) => [turn.content_type, turn.bytes]));
    const tampered = {
      ...evidence,
      per_turn: evidence.per_turn.map((turn, index) =>
        index === 0 ? { ...turn, sha256_hex: "0".repeat(64) } : turn,
      ),
    };
    await writeFile(evidencePath, JSON.stringify(tampered), { mode: 0o600 });
    await expect(
      ledger.ingestPair({
        provider: "chatgpt",
        profileId: "fixture-profile",
        rawPath,
        evidencePath,
        independentPath,
      }),
    ).rejects.toThrow(/body hash mismatch/);
    ledger.close();
  });

  it("serializes two independent publishers without sweeping either committed object", async () => {
    const dir = await tempRoot();
    const first = await fixture(dir, "publisher-a", "first");
    const second = await fixture(dir, "publisher-b", "second");
    const root = path.join(dir, "ledger");
    const firstLedger = await TranscriptLedger.open({ root });
    const secondLedger = await TranscriptLedger.open({ root });
    const [a, b] = await Promise.all([
      firstLedger.ingestPair({
        provider: "chatgpt",
        profileId: "p",
        rawPath: first.rawPath,
        evidencePath: first.evidencePath,
        independentPath: first.independentPath,
      }),
      secondLedger.ingestPair({
        provider: "chatgpt",
        profileId: "p",
        rawPath: second.rawPath,
        evidencePath: second.evidencePath,
        independentPath: second.independentPath,
      }),
    ]);
    expect(a.revisionId).not.toBe(b.revisionId);
    expect(firstLedger.list()[0]?.revisionCount).toBe(2);
    expect(firstLedger.list()[0]?.observationCount).toBe(2);
    expect(
      await readFile(path.join(root, "objects", "sha256", a.rawSha256.slice(0, 2), a.rawSha256)),
    ).toEqual(first.rawBytes);
    expect(
      await readFile(path.join(root, "objects", "sha256", b.rawSha256.slice(0, 2), b.rawSha256)),
    ).toEqual(second.rawBytes);
    firstLedger.close();
    secondLedger.close();
  });

  it("releases a queued turn when that ledger is closed and permits a fresh opener", async () => {
    const dir = await tempRoot();
    const second = await fixture(dir, "queued-second", "second");
    const root = path.join(dir, "ledger");
    const firstLedger = await TranscriptLedger.open({ root });
    const secondLedger = await TranscriptLedger.open({ root });
    const releaseBlock = await (
      firstLedger as unknown as {
        acquirePublicationTurnForOperation: () => Promise<() => void>;
      }
    ).acquirePublicationTurnForOperation();
    const secondPromise = secondLedger.ingestPair({
      provider: "chatgpt",
      profileId: "p",
      rawPath: second.rawPath,
      evidencePath: second.evidencePath,
      independentPath: second.independentPath,
    });
    for (
      let attempt = 0;
      attempt < 100 && (secondLedger as any).waitingOperations === 0;
      attempt += 1
    ) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    expect((secondLedger as any).waitingOperations).toBe(1);
    secondLedger.close();
    releaseBlock();
    await expect(secondPromise).rejects.toThrow(/closed/);
    firstLedger.close();
    const fresh = await TranscriptLedger.open({ root });
    expect(fresh.list()).toEqual([]);
    fresh.close();
  });

  it("defers active close until publication and crash-orphan recovery are complete", async () => {
    const dir = await tempRoot();
    const files = await fixture(dir, "active-close", "active");
    const root = path.join(dir, "ledger");
    const ledger = await TranscriptLedger.open({ root });
    const ingest = ledger.ingestPair({
      provider: "chatgpt",
      profileId: "p",
      rawPath: files.rawPath,
      evidencePath: files.evidencePath,
      independentPath: files.independentPath,
    });
    ledger.close();
    await expect(ingest).resolves.toBeTruthy();
    expect(() => ledger.list()).toThrow(/closed/);
    const fresh = await TranscriptLedger.open({ root });
    expect(fresh.list()[0]?.observationCount).toBe(1);
    fresh.close();
  });

  it("uses SQLite as the publication barrier across rollback and same-hash commit", async () => {
    const dir = await tempRoot();
    const files = await fixture(dir, "sqlite-barrier", "barrier");
    const root = path.join(dir, "ledger");
    const initial = await TranscriptLedger.open({ root });
    initial.close();
    const orphan = files.rawBytes;
    const digest = createHash("sha256").update(orphan).digest("hex");
    const objectDir = path.join(root, "objects", "sha256", digest.slice(0, 2));
    const relativeObjectPath = path.relative(root, path.join(objectDir, digest));
    await mkdir(objectDir, { recursive: true, mode: 0o700 });
    await writeFile(path.join(objectDir, digest), orphan, { mode: 0o600 });
    const writer = new DatabaseSync(path.join(root, "index.sqlite"));
    writer.exec("BEGIN IMMEDIATE");
    writer
      .prepare("INSERT INTO objects(sha256,kind,bytes,path,created_at) VALUES(?,?,?,?,?)")
      .run(
        digest,
        "raw-provider-json",
        orphan.byteLength,
        relativeObjectPath,
        new Date().toISOString(),
      );
    const moduleUrl = pathToFileURL(path.join(process.cwd(), "src/transcriptLedger.ts")).href;
    const childScript = `import { TranscriptLedger } from ${JSON.stringify(moduleUrl)}; const ledger = await TranscriptLedger.open({ root: ${JSON.stringify(root)} }); await ledger.ingestPair({ provider: "chatgpt", profileId: "p", rawPath: ${JSON.stringify(files.rawPath)}, evidencePath: ${JSON.stringify(files.evidencePath)}, independentPath: ${JSON.stringify(files.independentPath)} }); console.log("committed"); ledger.close();`;
    const child = spawn(
      process.execPath,
      ["--import", "tsx", "--input-type=module", "-e", childScript],
      {
        cwd: process.cwd(),
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    let childOutput = "";
    child.stdout.on("data", (chunk: Buffer) => {
      childOutput += chunk.toString("utf8");
    });
    const childExit = new Promise<number | null>((resolve) => {
      child.once("exit", (code) => resolve(code));
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(childOutput).not.toContain("committed");
    writer.exec("ROLLBACK");
    writer.close();
    expect(await childExit).toBe(0);
    expect(childOutput).toContain("committed");
    expect(await readFile(path.join(objectDir, digest))).toEqual(orphan);
  });

  it("repairs reopened object permissions and rejects a symlinked artifact ancestor", async () => {
    const dir = await tempRoot();
    const files = await fixture(dir, "permissions");
    const root = path.join(dir, "ledger");
    const ledger = await TranscriptLedger.open({ root });
    const result = await ledger.ingestPair({
      provider: "chatgpt",
      profileId: "p",
      rawPath: files.rawPath,
      evidencePath: files.evidencePath,
      independentPath: files.independentPath,
    });
    const object = path.join(
      root,
      "objects",
      "sha256",
      result.rawSha256.slice(0, 2),
      result.rawSha256,
    );
    await chmod(object, 0o644);
    ledger.close();
    const reopened = await TranscriptLedger.open({ root });
    expect((await lstat(object)).mode & 0o777).toBe(0o600);
    reopened.close();

    const outside = path.join(dir, "outside");
    await mkdir(outside, { mode: 0o700 });
    await symlink(outside, path.join(dir, "input-link"));
    await expect(
      TranscriptLedger.open({ root: path.join(dir, "other-ledger") }).then((other) =>
        other.ingestPair({
          provider: "chatgpt",
          profileId: "p",
          rawPath: path.join(dir, "input-link", path.basename(files.rawPath)),
          evidencePath: files.evidencePath,
          independentPath: files.independentPath,
        }),
      ),
    ).rejects.toThrow(/symlinks/);
  });
});
