import { describe, expect, it } from "vitest";
import { mkdir, readFile, symlink, writeFile, lstat } from "node:fs/promises";
import path from "node:path";
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
    independent_fetch: {
      document_sha256_decimal_bytes: "1 2 3",
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
  return { rawPath, evidencePath, rawBytes };
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
    });
    ledger.close();
    const digest = createHash("sha256").update(files.rawBytes).digest("hex");
    expect(result.rawSha256).toBe(digest);
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
    });
    const b = await ledger.ingestPair({
      provider: "chatgpt",
      profileId: "profile-a",
      rawPath: second.rawPath,
      evidencePath: second.evidencePath,
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
    });
    const b = await ledger.ingestPair({
      provider: "chatgpt",
      profileId: "profile-a",
      rawPath: two.rawPath,
      evidencePath: two.evidencePath,
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
});
