import { DatabaseSync } from "node:sqlite";
import { constants as fsConstants } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { chmod, lstat, mkdir, open, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { getOracleHomeDir } from "../oracleHome.js";

export const DURABLE_QUEUE_CAPABILITY_ID = "oracle.remote.durable-queue";
export const DURABLE_QUEUE_CAPABILITY_VERSION = 1;
export const DEFAULT_DURABLE_ACTIVE_CAPACITY = 4;
export const DEFAULT_DURABLE_BACKLOG = 8;
export const DURABLE_ETA_FLOOR_MS = 300_000;
export type DurableRunState =
  | "queued"
  | "running"
  | "completed"
  | "failed"
  | "canceled"
  | "unknown";
export type DurableRunPhase =
  | "accepted"
  | "dispatching"
  | "browser_attached"
  | "prompt_submitted"
  | "awaiting_response"
  | "capturing"
  | "terminal";
export interface DurableRunRequest {
  prompt: string;
  attachments?: unknown[];
  fallbackSubmission?: unknown;
  browserConfig: Record<string, unknown>;
  options?: Record<string, unknown>;
}
export interface DurableErrorMetadata {
  code?: string;
  type?: string;
  throttleMs?: number;
  message?: string;
}
export interface DurableRunSnapshot {
  id: string;
  state: DurableRunState;
  phase: DurableRunPhase;
  createdAt: string;
  updatedAt: string;
  queuePosition: number;
  roughEtaMs: number;
  requestHash: string;
  runtimeHint?: Record<string, unknown>;
  result?: unknown;
  error?: string;
  errorMetadata?: DurableErrorMetadata;
  failure?: DurableErrorMetadata;
  cancellation?: { requestedAt?: string; outcome?: string };
}
export interface DurableQueueOptions {
  homeDir?: string;
  capacity?: number;
  backlog?: number;
  now?: () => number;
}
type Row = Record<string, unknown>;
const terminal = new Set<DurableRunState>(["completed", "failed", "canceled", "unknown"]);
async function privateDirectory(dir: string): Promise<void> {
  const existing = await lstat(dir).catch(() => undefined);
  if (existing?.isSymbolicLink() || (existing && !existing.isDirectory()))
    throw new Error(`unsafe queue directory: ${dir}`);
  await mkdir(dir, { recursive: true, mode: 0o700 });
  await chmod(dir, 0o700);
  const s = await lstat(dir);
  if (!s.isDirectory() || s.isSymbolicLink()) throw new Error(`unsafe queue directory: ${dir}`);
}
async function safeDescendant(root: string, target: string, kind: string): Promise<void> {
  const rel = path.relative(root, target);
  if (rel.startsWith("..") || path.isAbsolute(rel)) throw new Error(`unsafe ${kind} path`);
  let cur = root;
  for (const part of rel.split(path.sep).filter(Boolean)) {
    cur = path.join(cur, part);
    const s = await lstat(cur).catch(() => undefined);
    if (!s) continue;
    if (s.isSymbolicLink() || (cur !== target && !s.isDirectory()))
      throw new Error(`unsafe ${kind} path`);
  }
}
function digest(v: unknown): string {
  return createHash("sha256").update(JSON.stringify(v)).digest("hex");
}
export class DurableQueueStore {
  readonly root: string;
  readonly dbPath: string;
  readonly capacity: number;
  readonly backlog: number;
  private readonly db: DatabaseSync;
  private readonly now: () => number;
  private constructor(db: DatabaseSync, root: string, o: DurableQueueOptions) {
    this.db = db;
    this.root = root;
    this.dbPath = path.join(root, "queue.sqlite");
    this.capacity = Math.max(1, Math.trunc(o.capacity ?? 4));
    this.backlog = Math.max(0, Math.trunc(o.backlog ?? 8));
    this.now = o.now ?? Date.now;
    const j = this.db.prepare("PRAGMA journal_mode=WAL").get() as Row;
    if (String(j.journal_mode).toLowerCase() !== "wal") throw new Error("SQLite WAL is required");
    this.db.exec("PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON");
    this.db.exec(
      `CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY,admission_seq INTEGER NOT NULL UNIQUE,idempotency_key TEXT NOT NULL UNIQUE,request_hash TEXT NOT NULL,request_path TEXT NOT NULL,state TEXT NOT NULL,phase TEXT NOT NULL,seq INTEGER NOT NULL DEFAULT 0,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,runtime_hint TEXT,result TEXT,error TEXT,error_meta TEXT,cancellation TEXT,model TEXT,eta_qualifying INTEGER NOT NULL DEFAULT 0,elapsed_ms INTEGER);CREATE TABLE IF NOT EXISTS events(run_id TEXT NOT NULL REFERENCES runs(id),seq INTEGER NOT NULL,created_at INTEGER NOT NULL,event TEXT NOT NULL,PRIMARY KEY(run_id,seq));CREATE TABLE IF NOT EXISTS eta_samples(elapsed_ms INTEGER NOT NULL,created_at INTEGER NOT NULL,model TEXT NOT NULL,qualifying INTEGER NOT NULL DEFAULT 1)`,
    );
    this.reconcile();
  }
  static async open(o: DurableQueueOptions = {}): Promise<DurableQueueStore> {
    const home = o.homeDir ?? getOracleHomeDir();
    const root = path.join(home, "remote-queue");
    await privateDirectory(home);
    await privateDirectory(root);
    await privateDirectory(path.join(root, "runs"));
    const dbPath = path.join(root, "queue.sqlite");
    await safeDescendant(root, dbPath, "database");
    await safeDescendant(root, `${dbPath}-wal`, "WAL");
    const db = new DatabaseSync(dbPath);
    await chmod(dbPath, 0o600);
    for (const suffix of ["-wal", "-shm"]) {
      const p = `${dbPath}${suffix}`;
      if (await lstat(p).catch(() => undefined)) {
        await safeDescendant(root, p, suffix.slice(1));
        await chmod(p, 0o600);
      }
    }
    return new DurableQueueStore(db, root, o);
  }
  close(): void {
    if (this.db.isOpen) this.db.close();
  }
  private append(id: string, t: number, event: unknown): void {
    const r = this.db.prepare("SELECT seq FROM runs WHERE id=?").get(id) as Row;
    const seq = Number(r.seq);
    this.db
      .prepare("INSERT INTO events(run_id,seq,created_at,event) VALUES(?,?,?,?)")
      .run(id, seq, t, JSON.stringify(event));
    this.db.prepare("UPDATE runs SET seq=? WHERE id=?").run(seq + 1, id);
  }
  private reconcile(): void {
    const rows = this.db.prepare("SELECT id FROM runs WHERE state='running'").all() as Row[];
    for (const r of rows) {
      const id = String(r.id),
        t = this.now();
      this.db.exec("BEGIN IMMEDIATE");
      try {
        this.db
          .prepare(
            "UPDATE runs SET state='unknown',phase='terminal',updated_at=?,error=?,error_meta=? WHERE id=?",
          )
          .run(
            t,
            "run interrupted by restart",
            JSON.stringify({ code: "restart_interrupted", type: "unknown" }),
            id,
          );
        this.append(id, t, {
          type: "state",
          state: "unknown",
          phase: "terminal",
          reason: "restart_interrupted",
        });
        this.db.exec("COMMIT");
      } catch (e) {
        this.db.exec("ROLLBACK");
        throw e;
      }
    }
  }
  private eta(): number {
    const rows = this.db
      .prepare(
        "SELECT elapsed_ms FROM eta_samples WHERE qualifying=1 AND model='pro' ORDER BY created_at DESC,rowid DESC LIMIT 20",
      )
      .all() as Row[];
    return Math.max(
      300000,
      rows.length
        ? Math.round(rows.reduce((a, r) => a + Number(r.elapsed_ms), 0) / rows.length)
        : 300000,
    );
  }
  private snapshot(r: Row): DurableRunSnapshot {
    const q = this.db
      .prepare("SELECT id FROM runs WHERE state='queued' ORDER BY admission_seq")
      .all() as Row[];
    const p = q.findIndex((x) => String(x.id) === String(r.id));
    return {
      id: String(r.id),
      state: r.state as DurableRunState,
      phase: r.phase as DurableRunPhase,
      createdAt: new Date(Number(r.created_at)).toISOString(),
      updatedAt: new Date(Number(r.updated_at)).toISOString(),
      queuePosition: p < 0 ? 0 : p + 1,
      roughEtaMs: p < 0 ? 0 : Math.max(300000, this.eta() * Math.ceil((p + 1) / this.capacity)),
      requestHash: String(r.request_hash),
      ...(r.runtime_hint ? { runtimeHint: JSON.parse(String(r.runtime_hint)) } : {}),
      ...(r.result ? { result: JSON.parse(String(r.result)) } : {}),
      ...(r.error ? { error: String(r.error) } : {}),
      ...(r.error_meta ? { errorMetadata: JSON.parse(String(r.error_meta)) } : {}),
      ...(r.error_meta ? { failure: JSON.parse(String(r.error_meta)) } : {}),
      ...(r.cancellation ? { cancellation: JSON.parse(String(r.cancellation)) } : {}),
    };
  }
  async submit(key: string, request: DurableRunRequest): Promise<DurableRunSnapshot> {
    if (!key || key.length > 512) throw new Error("stable Idempotency-Key is required");
    const hash = digest(request);
    const prior = this.db.prepare("SELECT * FROM runs WHERE idempotency_key=?").get(key) as
      | Row
      | undefined;
    if (prior) {
      if (prior.request_hash !== hash)
        throw new Error("idempotency key conflicts with an existing request");
      return this.snapshot(prior);
    }
    const id = randomUUID(),
      t = this.now(),
      dir = path.join(this.root, "runs", id);
    await privateDirectory(dir);
    const rp = path.join(dir, "request.json"),
      tmp = `${rp}.part-${randomUUID()}`;
    await writeFile(tmp, JSON.stringify(request), { mode: 0o600 });
    await chmod(tmp, 0o600);
    const file = await open(tmp, "r");
    await file.sync();
    await file.close();
    await rename(tmp, rp);
    await chmod(rp, 0o600);
    const parent = await open(dir, "r");
    await parent.sync();
    await parent.close();
    let committed = false;
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const old = this.db.prepare("SELECT * FROM runs WHERE idempotency_key=?").get(key) as
        | Row
        | undefined;
      if (old) {
        this.db.exec("ROLLBACK");
        await rm(dir, { recursive: true, force: true });
        if (old.request_hash !== hash)
          throw new Error("idempotency key conflicts with an existing request");
        return this.snapshot(old);
      }
      const total = Number(
        (
          this.db
            .prepare(
              "SELECT count(*) n FROM runs WHERE state NOT IN ('completed','failed','canceled','unknown')",
            )
            .get() as Row
        ).n,
      );
      if (total >= this.capacity + this.backlog) {
        this.db.exec("ROLLBACK");
        await rm(dir, { recursive: true, force: true });
        throw new Error("queue_full");
      }
      this.db
        .prepare(
          "INSERT INTO runs(id,admission_seq,idempotency_key,request_hash,request_path,state,phase,created_at,updated_at) VALUES(?,COALESCE((SELECT max(admission_seq)+1 FROM runs),1),?,?,?,?,?,?,?)",
        )
        .run(id, key, hash, rp, "queued", "accepted", t, t);
      this.append(id, t, { type: "accepted" });
      this.db.exec("COMMIT");
      committed = true;
      return this.snapshot(this.db.prepare("SELECT * FROM runs WHERE id=?").get(id) as Row);
    } catch (e) {
      try {
        this.db.exec("ROLLBACK");
      } catch {}
      if (!committed) await rm(dir, { recursive: true, force: true }).catch(() => undefined);
      throw e;
    }
  }
  get(id: string): DurableRunSnapshot | undefined {
    const r = this.db.prepare("SELECT * FROM runs WHERE id=?").get(id) as Row | undefined;
    return r ? this.snapshot(r) : undefined;
  }
  getByIdempotencyKey(k: string): DurableRunSnapshot | undefined {
    const r = this.db.prepare("SELECT * FROM runs WHERE idempotency_key=?").get(k) as
      | Row
      | undefined;
    return r ? this.snapshot(r) : undefined;
  }
  events(id: string, after = -1): Array<{ seq: number; event: unknown }> {
    return (
      this.db
        .prepare("SELECT seq,event FROM events WHERE run_id=? AND seq>? ORDER BY seq")
        .all(id, after) as Row[]
    ).map((r) => ({ seq: Number(r.seq), event: JSON.parse(String(r.event)) }));
  }
  appendEvent(id: string, event: unknown): void {
    const t = this.now();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.append(id, t, event);
      this.db.exec("COMMIT");
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }
  cancel(id: string): DurableRunSnapshot | undefined {
    const r = this.db.prepare("SELECT * FROM runs WHERE id=?").get(id) as Row | undefined;
    if (!r) return;
    const t = this.now();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const out = r.state === "queued" ? "canceled" : "requested";
      (
        this.db.prepare(
          "UPDATE runs SET cancellation=?,updated_at=?,state=?,phase=? WHERE id=?",
        ) as any
      ).run(
        JSON.stringify({ requestedAt: new Date(t).toISOString(), outcome: out }),
        t,
        r.state === "queued" ? "canceled" : String(r.state),
        r.state === "queued" ? "terminal" : String(r.phase),
        id,
      );
      this.append(id, t, { type: "cancellation", outcome: out });
      this.db.exec("COMMIT");
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
    return this.get(id);
  }
  transition(
    id: string,
    state: DurableRunState,
    phase: DurableRunPhase,
    p: {
      result?: unknown;
      error?: string;
      errorMetadata?: DurableErrorMetadata;
      runtimeHint?: Record<string, unknown>;
      elapsedMs?: number;
      model?: string;
      etaQualifying?: boolean;
    } = {},
  ): void {
    const o = this.db.prepare("SELECT * FROM runs WHERE id=?").get(id) as Row | undefined;
    if (!o) throw new Error("unknown run");
    if (terminal.has(o.state as DurableRunState) && o.state === state) return;
    if (terminal.has(o.state as DurableRunState)) throw new Error("terminal run cannot transition");
    const t = this.now(),
      hint =
        p.runtimeHint === undefined
          ? o.runtime_hint
            ? JSON.parse(String(o.runtime_hint))
            : null
          : { ...(o.runtime_hint ? JSON.parse(String(o.runtime_hint)) : {}), ...p.runtimeHint };
    this.db.exec("BEGIN IMMEDIATE");
    try {
      (
        this.db.prepare(
          "UPDATE runs SET state=?,phase=?,updated_at=?,result=?,error=?,error_meta=?,runtime_hint=?,elapsed_ms=?,model=?,eta_qualifying=? WHERE id=?",
        ) as any
      ).run(
        state,
        phase,
        t,
        p.result === undefined ? (o.result ?? null) : JSON.stringify(p.result),
        p.error === undefined ? (o.error ?? null) : p.error,
        p.errorMetadata === undefined ? (o.error_meta ?? null) : JSON.stringify(p.errorMetadata),
        hint ? JSON.stringify(hint) : null,
        p.elapsedMs === undefined ? (o.elapsed_ms ?? null) : p.elapsedMs,
        p.model ?? (String(o.model ?? "") || null),
        p.etaQualifying === undefined ? (o.eta_qualifying ?? 0) : p.etaQualifying ? 1 : 0,
        id,
      );
      this.append(id, t, { type: "state", state, phase });
      if (
        terminal.has(state) &&
        p.elapsedMs !== undefined &&
        state !== "canceled" &&
        state !== "unknown" &&
        p.etaQualifying === true
      )
        this.db.prepare("INSERT INTO eta_samples VALUES(?,?,?,1)").run(p.elapsedMs, t, "pro");
      this.db.exec("COMMIT");
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }
  claimNext(): DurableRunSnapshot | undefined {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const a = Number(
        (this.db.prepare("SELECT count(*) n FROM runs WHERE state='running'").get() as Row).n,
      );
      if (a >= this.capacity) {
        this.db.exec("ROLLBACK");
        return;
      }
      const r = this.db
        .prepare("SELECT * FROM runs WHERE state='queued' ORDER BY admission_seq LIMIT 1")
        .get() as Row | undefined;
      if (!r) {
        this.db.exec("ROLLBACK");
        return;
      }
      const t = this.now(),
        id = String(r.id);
      this.db
        .prepare("UPDATE runs SET state='running',phase='dispatching',updated_at=? WHERE id=?")
        .run(t, id);
      this.append(id, t, { type: "state", state: "running", phase: "dispatching" });
      this.db.exec("COMMIT");
      return this.get(id);
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }
  status() {
    const c = (s: string) =>
      Number((this.db.prepare("SELECT count(*) n FROM runs WHERE state=?").get(s) as Row).n);
    return {
      active: c("running"),
      queued: c("queued"),
      capacity: this.capacity,
      backlog: this.backlog,
      roughEtaMs: this.eta(),
    };
  }
  async request(id: string): Promise<DurableRunRequest | undefined> {
    const r = this.db.prepare("SELECT request_path FROM runs WHERE id=?").get(id) as
      | Row
      | undefined;
    if (!r) return;
    const p = String(r.request_path);
    await safeDescendant(this.root, p, "request");
    const file = await open(p, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
    try {
      return JSON.parse(await file.readFile("utf8")) as DurableRunRequest;
    } finally {
      await file.close();
    }
  }
  runDirectory(id: string): string {
    if (!/^[a-zA-Z0-9_-]{1,128}$/.test(id)) throw new Error("invalid run id");
    return path.join(this.root, "runs", id);
  }
}
