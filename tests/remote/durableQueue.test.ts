import { describe, expect, it } from "vitest";
import { mkdtemp, stat } from "node:fs/promises";
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
    store.transition(first.id, "completed", "terminal", { elapsedMs: 600000, etaQualifying: true });
    const second = await store.submit("eta-2", request("2"));
    const third = await store.submit("eta-3", request("3"));
    expect(third.roughEtaMs).toBe(600000);
    expect(second.roughEtaMs).toBe(600000);
    store.close();
  });
});
