import { describe, expect, it } from "vitest";
import { mkdir, readFile, symlink, writeFile, lstat } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { mkdtemp } from "node:fs/promises";
import { createHash } from "node:crypto";
import { TranscriptLedger, buildCaptureOnlySyncRequest } from "../src/transcriptLedger.js";

async function fixture(dir: string, suffix: string, body = "hello") {
  const raw = {
    conversation_id: "conv-1",
    current_node: "a2",
    create_time: Date.now(),
    mapping: {
      root: { id: "root", parent: null, message: null },
      a1: {
        id: "a1",
        parent: "root",
        message: {
          author: { role: "user" },
          content: { content_type: "text", parts: ["question"] },
        },
      },
      a2: {
        id: "a2",
        parent: "a1",
        message: {
          author: { role: "assistant" },
          content: { content_type: "text", parts: [body] },
        },
      },
    },
  };
  const rawPath = path.join(dir, `${suffix}-raw.json`);
  const evidencePath = path.join(dir, `${suffix}-evidence.json`);
  await writeFile(rawPath, JSON.stringify(raw));
  await writeFile(
    evidencePath,
    JSON.stringify({
      schema: "oracle.provider-native-capture-evidence/v1",
      conversation_id: "conv-1",
      per_turn: [],
    }),
  );
  return { rawPath, evidencePath, rawBytes: Buffer.from(JSON.stringify(raw)) };
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
});
