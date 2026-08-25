import { describe, expect, it } from "vitest";
import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readlink,
  rename,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  DurableQueueStore,
  DURABLE_ETA_FLOOR_MS,
  type DurableRunSnapshot,
} from "../../src/remote/durableQueue.js";

const request = (prompt: string, captureOnly = false) => ({
  prompt,
  browserConfig: { captureOnly, desiredModel: "gpt-5-pro" },
  options: {},
});

const maintenanceCaptureRequest = (conversationUrl: string, grantId: string) => ({
  prompt: "",
  attachments: [],
  browserConfig: {
    chatgptUrl: conversationUrl,
    url: conversationUrl,
    resumeConversationUrl: conversationUrl,
    captureOnly: true,
    captureProviderNative: true,
  },
  options: { sessionId: `maintenance-capture-${grantId}` },
});

describe("DurableQueueStore", () => {
  it("durably publishes request bytes before returning and reopens with WAL", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-queue-test-"));
    const store = await DurableQueueStore.open({ homeDir: home });
    const accepted = await store.submit("stable-1", request("hello"));
    const bytes = await store.request(accepted.id);
    expect(bytes?.prompt).toBe("hello");
    expect((await stat(store.root)).mode & 0o777).toBe(0o700);
    expect((await stat(store.dbPath)).mode & 0o777).toBe(0o600);
    store.close();
    const reopened = await DurableQueueStore.open({ homeDir: home });
    expect(reopened.get(accepted.id)?.id).toBe(accepted.id);
    expect(reopened.events(accepted.id)).toHaveLength(1);
    reopened.close();
  });

  it("is idempotent and rejects conflicting payloads", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-queue-test-"));
    const store = await DurableQueueStore.open({ homeDir: home });
    const first = await store.submit("same", request("one"));
    const second = await store.submit("same", request("one"));
    expect(second.id).toBe(first.id);
    await expect(store.submit("same", request("two"))).rejects.toThrow(/conflicts/);
    store.close();
  });

  it("keeps strict FIFO, capacity four, backlog eight, and ETA floor", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-queue-test-"));
    const store = await DurableQueueStore.open({ homeDir: home, capacity: 4, backlog: 8 });
    const rows: DurableRunSnapshot[] = [];
    for (let i = 0; i < 4; i++) rows.push(await store.submit(`k-${i}`, request(String(i))));
    rows.forEach(() => store.claimNext());
    const queued = [];
    for (let i = 0; i < 8; i++) queued.push(await store.submit(`q-${i}`, request(`q-${i}`)));
    expect(store.status()).toMatchObject({ active: 4, queued: 8, capacity: 4, backlog: 8 });
    expect(queued[0]?.queuePosition).toBe(1);
    expect(queued[7]?.roughEtaMs).toBeGreaterThanOrEqual(DURABLE_ETA_FLOOR_MS);
    await expect(store.submit("overflow", request("overflow"))).rejects.toThrow("queue_full");
    for (let i = 0; i < 4; i++) expect(store.claimNext()?.id).toBeUndefined();
    store.close();
  });

  it("reconciles in-flight rows to unknown and never replays them", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-queue-test-"));
    const store = await DurableQueueStore.open({ homeDir: home });
    const row = await store.submit("inflight", request("x"));
    expect(store.claimNext()?.id).toBe(row.id);
    store.close();
    const reopened = await DurableQueueStore.open({ homeDir: home });
    expect(reopened.get(row.id)?.state).toBe("unknown");
    expect(reopened.claimNext()).toBeUndefined();
    reopened.close();
  });

  it("admits exactly capacity plus backlog before pumping", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-queue-test-"));
    const store = await DurableQueueStore.open({ homeDir: home, capacity: 4, backlog: 8 });
    for (let i = 0; i < 12; i++) await store.submit(`full-${i}`, request(`${i}`));
    await expect(store.submit("full-13", request("13"))).rejects.toThrow("queue_full");
    expect(store.status()).toMatchObject({ active: 0, queued: 12 });
    store.close();
  });

  it("keeps fixed-clock admission FIFO and merges runtime hints", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-queue-test-"));
    const store = await DurableQueueStore.open({ homeDir: home, capacity: 1, now: () => 1000 });
    const rows = [];
    for (const key of ["a", "b", "c"]) rows.push(await store.submit(key, request(key)));
    expect(store.claimNext()?.id).toBe(rows[0]?.id);
    store.transition(rows[0]!.id, "completed", "terminal", {
      result: { ok: true },
      runtimeHint: { phase: "done" },
    });
    const next = store.claimNext();
    expect(next?.id).toBe(rows[1]?.id);
    expect(store.get(rows[0]!.id)?.result).toEqual({ ok: true });
    expect(store.get(rows[0]!.id)?.runtimeHint).toEqual({ phase: "done" });
    store.close();
  });

  it("uses capacity-sized ETA waves and qualified samples only", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-queue-test-"));
    const store = await DurableQueueStore.open({ homeDir: home, capacity: 2 });
    const first = await store.submit("eta-1", request("1"));
    store.transition(first.id, "completed", "terminal", {
      elapsedMs: 600000,
      model: "gpt-5.5-pro",
      etaQualifying: true,
    });
    const second = await store.submit("eta-2", request("2"));
    const third = await store.submit("eta-3", request("3"));
    expect(third.roughEtaMs).toBe(600000);
    expect(second.roughEtaMs).toBe(600000);
    store.close();
  });

  it("arbitrates cancellation and completion by transaction order", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-queue-test-"));
    const store = await DurableQueueStore.open({ homeDir: home });
    const first = await store.submit("cancel-first", request("cancel-first"));
    expect(store.cancel(first.id)?.state).toBe("canceled");
    expect(store.cancel(first.id)?.cancellation?.outcome).toBe("canceled");
    expect(store.claimNext()).toBeUndefined();
    expect(() => store.transition(first.id, "completed", "terminal", { result: 1 })).toThrow(
      /terminal/,
    );

    const second = await store.submit("complete-first", request("complete-first"));
    expect(store.claimNext()?.id).toBe(second.id);
    store.transition(second.id, "completed", "terminal", { result: 2 });
    expect(store.cancel(second.id)?.result).toBe(2);
    expect(store.get(second.id)?.state).toBe("completed");
    expect(store.events(second.id).map((e) => e.seq)).toEqual([0, 1, 2]);
    store.close();
  });

  it("turns post-submit cancellation into terminal unknown and keeps queued rows unclaimable", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-queue-test-"));
    const store = await DurableQueueStore.open({ homeDir: home, capacity: 1 });
    const running = await store.submit("post-submit", request("post-submit"));
    expect(store.claimNext()?.id).toBe(running.id);
    store.transition(running.id, "running", "prompt_submitted");
    expect(store.cancel(running.id)).toMatchObject({ state: "unknown", phase: "terminal" });
    expect(store.get(running.id)?.cancellation?.outcome).toBe("unknown");

    const queued = await store.submit("reopen-queued", request("reopen-queued"));
    store.close();
    const reopened = await DurableQueueStore.open({ homeDir: home, capacity: 1 });
    expect(reopened.get(queued.id)?.state).toBe("queued");
    expect(reopened.claimNext()?.id).toBe(queued.id);
    reopened.close();
  });

  it("acquires the idle fence atomically, preserves exact replay, and survives reopen", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-maintenance-fence-"));
    const store = await DurableQueueStore.open({ homeDir: home });
    const existing = await store.submit("existing", request("existing"));
    expect(() => store.beginDrainIfIdle("drain-busy", "require-idle")).toThrow("bridge_busy");
    expect(store.admission()).toEqual({ state: "open" });
    store.cancel(existing.id);

    const drain = store.beginDrainIfIdle("drain-stable", "require-idle");
    expect(drain).toMatchObject({ state: "draining", activeRuns: 0, queuedRuns: 0 });
    expect(store.beginDrainIfIdle("drain-stable", "require-idle")).toEqual(drain);
    await expect(store.submit("new-during-drain", request("blocked"))).rejects.toThrow(
      "admission_draining",
    );
    await expect(store.submit("existing", request("existing"))).resolves.toMatchObject({
      id: existing.id,
    });
    store.close();

    const reopened = await DurableQueueStore.open({ homeDir: home });
    expect(reopened.admission()).toMatchObject({ state: "draining", drainId: drain.drainId });
    expect(reopened.getDrain(drain.drainId)).toEqual(drain);
    expect(reopened.releaseDrain(drain.drainId)).toEqual({
      schemaVersion: 1,
      drainId: drain.drainId,
      state: "open",
    });
    reopened.close();
  });

  it("stores only a capture token digest and binds one key with exact replay", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-capture-grant-"));
    const store = await DurableQueueStore.open({ homeDir: home });
    const drain = store.beginDrainIfIdle("drain-capture", "require-idle");
    const conversationUrl = "https://chatgpt.com/c/accepted-conversation";
    const grant = store.issueCaptureGrant(
      drain.drainId,
      "accepted-conversation",
      conversationUrl,
      "issue-capture-key",
      "operator-secret",
    );
    expect(
      store.issueCaptureGrant(
        drain.drainId,
        "accepted-conversation",
        conversationUrl,
        "issue-capture-key",
        "operator-secret",
      ),
    ).toEqual({ ...grant, replayed: true });
    expect(() =>
      store.issueCaptureGrant(
        drain.drainId,
        "different-conversation",
        "https://chatgpt.com/c/different-conversation",
        "issue-capture-key",
        "operator-secret",
      ),
    ).toThrow("capture_grant_issue_conflict");
    const payload = maintenanceCaptureRequest(conversationUrl, grant.grantId);
    const accepted = await store.submitGrantedCapture(grant.token, "capture-key", payload);
    await expect(
      store.submitGrantedCapture(grant.token, "capture-key", payload),
    ).resolves.toMatchObject({ id: accepted.id });
    await expect(store.submitGrantedCapture(grant.token, "second-key", payload)).rejects.toThrow(
      "capture_grant_consumed",
    );
    expect(store.getCaptureGrant(grant.grantId)).toMatchObject({
      state: "admitted",
      runId: accepted.id,
    });
    expect(() => store.transition(accepted.id, "completed", "terminal")).toThrow(
      "verified bridge evidence",
    );
    store.transition(accepted.id, "completed", "terminal", {
      captureAudit: {
        artifactManifestSha256: "a".repeat(64),
        artifactCount: 0,
        submissionAttempted: false,
        promptSubmitted: false,
      },
    });
    expect(store.getCaptureGrant(grant.grantId)).toMatchObject({
      state: "completed",
      terminal: {
        state: "completed",
        verified: true,
        submissionAttempted: false,
        promptSubmitted: false,
        artifactCount: 0,
        artifactManifestSha256: "a".repeat(64),
      },
    });
    for (const file of [store.dbPath, `${store.dbPath}-wal`, `${store.dbPath}-shm`]) {
      const bytes = await readFile(file).catch(() => Buffer.alloc(0));
      expect(bytes.includes(Buffer.from(grant.token))).toBe(false);
    }
    expect(store.releaseDrain(drain.drainId).state).toBe("open");
    store.close();
  });

  it("fails an interrupted granted capture without reopening its durable fence", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-capture-reconcile-"));
    const store = await DurableQueueStore.open({ homeDir: home });
    const drain = store.beginDrainIfIdle("drain-restart", "require-idle");
    const conversationUrl = "https://chatgpt.com/c/restart-conversation";
    const grant = store.issueCaptureGrant(
      drain.drainId,
      "restart-conversation",
      conversationUrl,
      "issue-restart-key",
      "operator-secret",
    );
    const accepted = await store.submitGrantedCapture(
      grant.token,
      "restart-key",
      maintenanceCaptureRequest(conversationUrl, grant.grantId),
    );
    expect(store.claimNext()?.id).toBe(accepted.id);
    store.close();

    const reopened = await DurableQueueStore.open({ homeDir: home });
    expect(reopened.get(accepted.id)).toMatchObject({ state: "unknown", phase: "terminal" });
    expect(reopened.getCaptureGrant(grant.grantId)).toMatchObject({
      state: "failed",
      terminal: { state: "unknown", failureCode: "restart_interrupted" },
    });
    expect(reopened.admission()).toMatchObject({ state: "draining", drainId: drain.drainId });
    expect(reopened.releaseDrain(drain.drainId).state).toBe("open");
    reopened.close();
  });

  it("atomically revokes a never-admitted grant when its idle drain is released", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-capture-unused-release-"));
    const store = await DurableQueueStore.open({ homeDir: home });
    const drain = store.beginDrainIfIdle("unused-drain", "require-idle");
    const grant = store.issueCaptureGrant(
      drain.drainId,
      "unused-conversation",
      "https://chatgpt.com/c/unused-conversation",
      "unused-grant",
      "operator-secret",
    );
    expect(store.getCaptureGrant(grant.grantId)).toMatchObject({ state: "issued" });
    expect(store.releaseDrain(drain.drainId)).toEqual({
      schemaVersion: 1,
      drainId: drain.drainId,
      state: "open",
    });
    expect(store.getCaptureGrant(grant.grantId)).toMatchObject({
      state: "revoked",
      terminal: {
        state: "canceled",
        verified: false,
        submissionAttempted: false,
        promptSubmitted: false,
        failureCode: "maintenance_drain_released",
      },
    });
    expect(store.authorizeCaptureGrant(grant.token)).toMatchObject({ active: false });
    store.close();
  });

  it("uses exactly the last 20 verified pro samples and excludes other terminals", async () => {
    const home = await mkdtemp(path.join(os.tmpdir(), "oracle-queue-test-"));
    const store = await DurableQueueStore.open({ homeDir: home, capacity: 1 });
    for (let i = 0; i < 25; i++) {
      const row = await store.submit(`eta-${i}`, request(String(i)));
      store.transition(row.id, "completed", "terminal", {
        elapsedMs: 400_000 + i * 10_000,
        model: i === 1 ? "gpt-5" : "gpt-5.5-pro",
        etaQualifying: true,
      });
    }
    const nonqualifying = await store.submit("eta-nonqualifying", request("nonqualifying"));
    store.transition(nonqualifying.id, "completed", "terminal", {
      elapsedMs: 1,
      model: "gpt-5.5-pro",
      etaQualifying: false,
    });
    const nonpro = await store.submit("eta-nonpro", request("nonpro"));
    store.transition(nonpro.id, "completed", "terminal", {
      elapsedMs: 1,
      model: "gpt-5",
      etaQualifying: true,
    });
    const canceled = await store.submit("eta-canceled", request("canceled"));
    store.transition(canceled.id, "canceled", "terminal", {
      elapsedMs: 99_999_999,
      model: "gpt-5.5-pro",
      etaQualifying: true,
    });
    const unknown = await store.submit("eta-unknown", request("unknown"));
    store.transition(unknown.id, "unknown", "terminal", {
      elapsedMs: 99_999_999,
      model: "gpt-5.5-pro",
      etaQualifying: true,
    });
    const failed = await store.submit("eta-failed", request("failed"));
    store.transition(failed.id, "failed", "terminal", {
      elapsedMs: 1,
      model: "gpt-5.5-pro",
      etaQualifying: true,
    });
    const queued = await store.submit("eta-queued", request("queued"));
    // Exactly the last 20 qualified pro samples are 450..640 seconds: mean 545 seconds.
    expect(queued.roughEtaMs).toBe(545_000);
    store.close();
  });

  it("rejects symlinked homes, roots, and request files and preserves private WAL files", async () => {
    const parent = await mkdtemp(path.join(os.tmpdir(), "oracle-queue-test-"));
    const real = path.join(parent, "real");
    const linked = path.join(parent, "linked");
    await symlink(real, linked);
    await expect(DurableQueueStore.open({ homeDir: linked })).rejects.toThrow(
      /unsafe queue directory/,
    );
    const rootLinkedHome = path.join(parent, "root-linked-home");
    await mkdir(rootLinkedHome, { mode: 0o700 });
    await symlink(real, path.join(rootLinkedHome, "remote-queue"));
    await expect(DurableQueueStore.open({ homeDir: rootLinkedHome })).rejects.toThrow(
      /unsafe queue directory/,
    );

    const store = await DurableQueueStore.open({ homeDir: real });
    const row = await store.submit("symlink-request", request("safe"));
    const requestPath = path.join(store.runDirectory(row.id), "request.json");
    const requestBackup = `${requestPath}.real`;
    await rename(requestPath, requestBackup);
    const external = path.join(parent, "external-request.json");
    await writeFile(external, JSON.stringify(request("external")), { mode: 0o600 });
    await symlink(external, requestPath);
    await expect(store.request(row.id)).rejects.toThrow();
    await rename(requestBackup, requestPath);
    const target = await readlink(requestPath).catch(() => undefined);
    expect(target).toBeUndefined();
    await chmod(requestPath, 0o600);
    const wal = `${store.dbPath}-wal`;
    const shm = `${store.dbPath}-shm`;
    for (const p of [wal, shm]) {
      const s = await lstat(p);
      expect(s.isSymbolicLink()).toBe(false);
      expect(s.mode & 0o777).toBe(0o600);
    }
    store.close();
  });
});
