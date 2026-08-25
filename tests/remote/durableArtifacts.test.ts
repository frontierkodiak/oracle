import { lstat, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  persistBrowserRunArtifacts,
  reopenDurableArtifactRun,
  resolveDurableArtifact,
} from "../../src/remote/durableArtifacts.js";
import type { BrowserRunResult } from "../../src/browserMode.js";

const result = (artifacts: NonNullable<BrowserRunResult["artifacts"]>): BrowserRunResult => ({
  answerText: "answer",
  answerMarkdown: "**answer**",
  tookMs: 1,
  answerTokens: 1,
  answerChars: 6,
  artifacts,
});

describe("durable remote artifacts", () => {
  it("copies every local artifact, hashes it, and removes source paths from the result", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "oracle-artifacts-"));
    const source = path.join(root, "provider result.json");
    await writeFile(source, '{"native":true}');
    const browserResult = {
      ...result([
        { kind: "file", path: source, mimeType: "application/json", sourceUrl: "sandbox:x" },
      ]),
      modelSelection: {
        status: "already-selected",
        verified: true,
        source: "config",
        capturedAt: "2026-01-01",
      },
      thinkingSelection: {
        requestedLevel: "standard",
        status: "already-selected",
        verified: true,
        strictFailClosed: true,
        source: "chatgpt-thinking-picker",
        capturedAt: "2026-01-01",
      },
      tabUrl: "https://chatgpt.com/c/abc",
      conversationId: "abc",
      promptSubmitted: true,
      chromePid: 42,
      chromePort: 9222,
      chromeHost: "127.0.0.1",
      chromeBrowserWSEndpoint: "ws://127.0.0.1/devtools",
      chromeProfileRoot: "/private/profile",
      userDataDir: "/private/user-data",
      chromeTargetId: "target",
      controllerPid: 99,
    } as BrowserRunResult;
    const run = await persistBrowserRunArtifacts({
      queueRoot: root,
      runId: "11111111-1111-4111-8111-111111111111",
      result: browserResult,
    });
    expect(run.descriptors).toHaveLength(1);
    expect(run.descriptors[0]).toMatchObject({
      filename: "provider_result.json",
      mimeType: "application/json",
      byteSize: 15,
      sourceUrlKind: "sandbox",
      transferStatus: "ready",
    });
    expect(run.descriptors[0].sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(JSON.stringify(run.result)).not.toContain(source);
    expect(run.result.thinkingSelection?.requestedLevel).toBe("standard");
    expect(JSON.stringify(JSON.parse(await readFile(run.manifestPath, "utf8")))).not.toContain(
      "/private/profile",
    );
    expect(JSON.parse(await readFile(run.manifestPath, "utf8")).artifacts).toHaveLength(1);
    expect(
      (await reopenDurableArtifactRun({ queueRoot: root, runId: run.runId })).result
        .thinkingSelection?.status,
    ).toBe("already-selected");
  });

  it("handles multiple and colliding names with private modes", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "oracle-artifacts-"));
    const a = path.join(root, "a");
    const b = path.join(root, "b");
    await writeFile(a, "one");
    await writeFile(b, "two");
    const run = await persistBrowserRunArtifacts({
      queueRoot: root,
      result: result([
        { kind: "file", path: a, label: "../same?.txt" },
        { kind: "file", path: b, label: "same?.txt" },
      ]),
    });
    expect(run.descriptors.map((d) => d.filename)).toEqual(["same_.txt", "same_.txt"]);
    expect(new Set(run.descriptors.map((d) => d.artifactId)).size).toBe(2);
    expect((await lstat(run.runRoot)).mode & 0o777).toBe(0o700);
    expect((await lstat(path.join(run.runRoot, "artifacts"))).mode & 0o777).toBe(0o700);
    const resolved = await resolveDurableArtifact({
      queueRoot: root,
      runId: run.runId,
      artifactId: run.descriptors[0].artifactId,
    });
    expect((await lstat(resolved.filePath)).mode & 0o777).toBe(0o600);
  });

  it("reopens and validates the authoritative manifest", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "oracle-artifacts-"));
    const source = path.join(root, "x.txt");
    await writeFile(source, "hello");
    const run = await persistBrowserRunArtifacts({
      queueRoot: root,
      result: result([{ kind: "file", path: source }]),
    });
    const reopened = await reopenDurableArtifactRun({ queueRoot: root, runId: run.runId });
    expect(reopened.descriptors).toEqual(run.descriptors);
    await writeFile(
      path.join(
        run.runRoot,
        "artifacts",
        `${run.descriptors[0].artifactId}-${run.descriptors[0].filename}`,
      ),
      "tampered",
    );
    await expect(reopenDurableArtifactRun({ queueRoot: root, runId: run.runId })).rejects.toThrow(
      /hash|size/,
    );
  });

  it("fails closed for symlink inputs and symlinked stored files", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "oracle-artifacts-"));
    const target = path.join(root, "target");
    const link = path.join(root, "link");
    await writeFile(target, "x");
    await symlink(target, link);
    await expect(
      persistBrowserRunArtifacts({
        queueRoot: root,
        result: result([{ kind: "file", path: link }]),
      }),
    ).rejects.toThrow(/regular/);
    const run = await persistBrowserRunArtifacts({
      queueRoot: root,
      result: result([{ kind: "file", path: target }]),
    });
    const stored = path.join(
      run.runRoot,
      "artifacts",
      `${run.descriptors[0].artifactId}-${run.descriptors[0].filename}`,
    );
    await rm(stored);
    await symlink(target, stored);
    await expect(
      resolveDurableArtifact({
        queueRoot: root,
        runId: run.runId,
        artifactId: run.descriptors[0].artifactId,
      }),
    ).rejects.toThrow(/symlink|identity|regular/);
  });

  it("cleans up a partially written run when a later source is invalid", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "oracle-artifacts-"));
    const source = path.join(root, "ok");
    await writeFile(source, "ok");
    const missing = path.join(root, "missing");
    const runId = "22222222-2222-4222-8222-222222222222";
    await expect(
      persistBrowserRunArtifacts({
        queueRoot: root,
        runId,
        result: result([
          { kind: "file", path: source },
          { kind: "file", path: missing },
        ]),
      }),
    ).rejects.toThrow();
    expect((await lstat(path.join(root, "runs", runId))).isDirectory()).toBe(true);
    await expect(lstat(path.join(root, "runs", runId, "artifacts"))).rejects.toThrow();
  });

  it("coexists with a queue-owned run directory and preserves request.json", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "oracle-artifacts-"));
    const runId = "33333333-3333-4333-8333-333333333333";
    const runRoot = path.join(root, "runs", runId);
    await mkdir(path.join(root, "runs"), { recursive: true });
    await mkdir(runRoot, { mode: 0o700 });
    await writeFile(path.join(runRoot, "request.json"), '{"prompt":"keep"}');
    const source = path.join(root, "x.txt");
    await writeFile(source, "hello");
    const saved = await persistBrowserRunArtifacts({
      queueRoot: root,
      runId,
      result: result([{ kind: "file", path: source }]),
    });
    expect(await readFile(path.join(runRoot, "request.json"), "utf8")).toBe('{"prompt":"keep"}');
    expect(saved.result.answerText).toBe("answer");
    expect((await reopenDurableArtifactRun({ queueRoot: root, runId })).result.answerText).toBe(
      "answer",
    );
  });

  it("rejects manifest symlinks and malformed descriptor/result fields", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "oracle-artifacts-"));
    const source = path.join(root, "x");
    await writeFile(source, "hello");
    const run = await persistBrowserRunArtifacts({
      queueRoot: root,
      result: result([{ kind: "file", path: source }]),
    });
    const manifest = JSON.parse(await readFile(run.manifestPath, "utf8"));
    await rm(run.manifestPath);
    await symlink(source, run.manifestPath);
    await expect(reopenDurableArtifactRun({ queueRoot: root, runId: run.runId })).rejects.toThrow(
      /manifest|symlink/,
    );
    await rm(run.manifestPath);
    for (const mutate of [
      (m: any) => {
        m.artifacts[0].filename = "../escape";
      },
      (m: any) => {
        m.artifacts[0].sha256 = "0".repeat(64);
      },
      (m: any) => {
        m.artifacts[0].byteSize += 1;
      },
      (m: any) => {
        m.artifacts[0].unexpected = true;
      },
      (m: any) => {
        m.result.unexpected = true;
      },
    ]) {
      const copy = structuredClone(manifest);
      mutate(copy);
      await writeFile(run.manifestPath, JSON.stringify(copy), { mode: 0o600 });
      await expect(
        reopenDurableArtifactRun({ queueRoot: root, runId: run.runId }),
      ).rejects.toThrow();
    }
  });

  it("rejects symlinked queue roots and stale artifact directories", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "oracle-artifacts-"));
    const target = path.join(root, "target");
    const linked = path.join(root, "linked");
    await mkdir(target);
    await symlink(target, linked);
    const source = path.join(root, "source");
    await writeFile(source, "x");
    await expect(
      persistBrowserRunArtifacts({
        queueRoot: linked,
        result: result([{ kind: "file", path: source }]),
      }),
    ).rejects.toThrow(/unsafe/);
    const runId = "44444444-4444-4444-8444-444444444444";
    const runRoot = path.join(root, "runs", runId);
    await mkdir(path.join(runRoot, "artifacts"), { recursive: true });
    await expect(
      persistBrowserRunArtifacts({
        queueRoot: root,
        runId,
        result: result([{ kind: "file", path: source }]),
      }),
    ).rejects.toThrow(/stale|already/);
  });
});
