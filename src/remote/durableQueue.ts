import { DatabaseSync } from "node:sqlite";
import { createHash, randomUUID } from "node:crypto";
import { chmod, lstat, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { getOracleHomeDir } from "../oracleHome.js";

export const DURABLE_QUEUE_CAPABILITY_ID = "oracle.remote.durable-queue";
export const DURABLE_QUEUE_CAPABILITY_VERSION = 1;
export const DEFAULT_DURABLE_ACTIVE_CAPACITY = 4;
export const DEFAULT_DURABLE_BACKLOG = 8;
export const DURABLE_ETA_FLOOR_MS = 300_000;

export type DurableRunState = "queued" | "running" | "completed" | "failed" | "canceled" | "unknown";
export type DurableRunPhase = "accepted" | "dispatching" | "browser_attached" | "prompt_submitted" | "awaiting_response" | "capturing" | "terminal";

export interface DurableRunRequest {
  prompt: string;
  attachments?: unknown[];
  fallbackSubmission?: unknown;
  browserConfig: Record<string, unknown>;
  options?: Record<string, unknown>;
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
}

export interface DurableQueueOptions {
  homeDir?: string;
  capacity?: number;
  backlog?: number;
  now?: () => number;
}

type Row = Record<string, unknown>;

function digest(request: unknown): string {
  return createHash("sha256").update(JSON.stringify(request)).digest("hex");
}

async function privateDirectory(dir: string): Promise<void> {
  await mkdir(dir, { recursive: true, mode: 0o700 });
  await chmod(dir, 0o700);
  const info = await lstat(dir);
  if (!info.isDirectory() || info.isSymbolicLink()) throw new Error(`unsafe queue directory: ${dir}`);
}

export class DurableQueueStore {
  readonly root: string;
  readonly dbPath: string;
  readonly capacity: number;
  readonly backlog: number;
  private readonly db: DatabaseSync;
  private readonly now: () => number;

  private constructor(db: DatabaseSync, root: string, options: DurableQueueOptions) {
    this.db = db;
    this.root = root;
    this.dbPath = path.join(root, "queue.sqlite");
    this.capacity = Math.max(1, Math.trunc(options.capacity ?? DEFAULT_DURABLE_ACTIVE_CAPACITY));
    this.backlog = Math.max(0, Math.trunc(options.backlog ?? DEFAULT_DURABLE_BACKLOG));
    this.now = options.now ?? Date.now;
    this.db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;");
    this.db.exec(`CREATE TABLE IF NOT EXISTS runs (
      id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE, request_hash TEXT NOT NULL,
      request_path TEXT NOT NULL, state TEXT NOT NULL, phase TEXT NOT NULL,
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, runtime_hint TEXT,
      result TEXT, error TEXT, model TEXT, elapsed_ms INTEGER
    );
    CREATE TABLE IF NOT EXISTS events (
      run_id TEXT NOT NULL REFERENCES runs(id), seq INTEGER NOT NULL,
      created_at INTEGER NOT NULL, event TEXT NOT NULL, PRIMARY KEY(run_id, seq)
    );
    CREATE TABLE IF NOT EXISTS eta_samples (elapsed_ms INTEGER NOT NULL, created_at INTEGER NOT NULL);`);
    this.reconcile();
  }

  static async open(options: DurableQueueOptions = {}): Promise<DurableQueueStore> {
    const root = path.join(options.homeDir ?? getOracleHomeDir(), "remote-queue");
    await privateDirectory(options.homeDir ?? getOracleHomeDir());
    await privateDirectory(root);
    const dbPath = path.join(root, "queue.sqlite");
    try {
      const info = await lstat(dbPath).catch(() => undefined);
      if (info && (!info.isFile() || info.isSymbolicLink())) throw new Error("unsafe queue database path");
    } catch (error) { throw error; }
    const db = new DatabaseSync(dbPath);
    await chmod(dbPath, 0o600).catch(() => undefined);
    return new DurableQueueStore(db, root, options);
  }

  close(): void { this.db.close(); }

  private reconcile(): void {
    const rows = this.db.prepare("SELECT id FROM runs WHERE state IN ('running','unknown')").all() as Row[];
    const now = this.now();
    const stmt = this.db.prepare("UPDATE runs SET state='unknown', phase='terminal', updated_at=? WHERE id=?");
    for (const row of rows) stmt.run(now, String(row.id));
  }

  private snapshot(row: Row): DurableRunSnapshot {
    const queue = this.db.prepare("SELECT id FROM runs WHERE state='queued' ORDER BY created_at, id").all() as Row[];
    const pos = queue.findIndex((entry) => entry.id === row.id);
    return {
      id: String(row.id), state: row.state as DurableRunState, phase: row.phase as DurableRunPhase,
      createdAt: new Date(Number(row.created_at)).toISOString(), updatedAt: new Date(Number(row.updated_at)).toISOString(),
      queuePosition: pos < 0 ? 0 : pos + 1, roughEtaMs: pos < 0 ? 0 : Math.max(DURABLE_ETA_FLOOR_MS, this.eta() * (pos + 1)),
      requestHash: String(row.request_hash),
      ...(row.runtime_hint ? { runtimeHint: JSON.parse(String(row.runtime_hint)) } : {}),
      ...(row.result ? { result: JSON.parse(String(row.result)) } : {}), ...(row.error ? { error: String(row.error) } : {}),
    };
  }

  private eta(): number {
    const rows = this.db.prepare("SELECT elapsed_ms FROM eta_samples ORDER BY created_at DESC LIMIT 20").all() as Row[];
    if (!rows.length) return DURABLE_ETA_FLOOR_MS;
    return Math.max(DURABLE_ETA_FLOOR_MS, Math.round(rows.reduce((sum, row) => sum + Number(row.elapsed_ms), 0) / rows.length));
  }

  async submit(idempotencyKey: string, request: DurableRunRequest): Promise<DurableRunSnapshot> {
    if (!idempotencyKey || idempotencyKey.length > 512) throw new Error("stable Idempotency-Key is required");
    const hash = digest(request);
    const existing = this.db.prepare("SELECT * FROM runs WHERE idempotency_key=?").get(idempotencyKey) as Row | undefined;
    if (existing) {
      if (existing.request_hash !== hash) throw new Error("idempotency key conflicts with an existing request");
      return this.snapshot(existing);
    }
    const queued = Number((this.db.prepare("SELECT count(*) AS n FROM runs WHERE state='queued'").get() as Row).n);
    const active = Number((this.db.prepare("SELECT count(*) AS n FROM runs WHERE state='running'").get() as Row).n);
    if (active >= this.capacity && queued >= this.backlog) throw new Error("queue_full");
    const id = randomUUID(); const t = this.now(); const runDir = path.join(this.root, "runs", id);
    await privateDirectory(path.join(this.root, "runs"));
    await privateDirectory(runDir);
    const requestPath = path.join(runDir, "request.json");
    const tempPath = `${requestPath}.part-${randomUUID()}`;
    await writeFile(tempPath, JSON.stringify(request), { mode: 0o600 });
    await chmod(tempPath, 0o600);
    await rename(tempPath, requestPath);
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.db.prepare("INSERT INTO runs (id,idempotency_key,request_hash,request_path,state,phase,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)")
        .run(id, idempotencyKey, hash, requestPath, "queued", "accepted", t, t);
      this.db.prepare("INSERT INTO events (run_id,seq,created_at,event) VALUES (?,?,?,?)").run(id, 0, t, JSON.stringify({ type: "accepted" }));
      this.db.exec("COMMIT");
    } catch (error) { this.db.exec("ROLLBACK"); await rm(runDir, { recursive: true, force: true }); throw error; }
    return this.snapshot(this.db.prepare("SELECT * FROM runs WHERE id=?").get(id) as Row);
  }

  get(id: string): DurableRunSnapshot | undefined { const row = this.db.prepare("SELECT * FROM runs WHERE id=?").get(id) as Row | undefined; return row ? this.snapshot(row) : undefined; }
  getByIdempotencyKey(key: string): DurableRunSnapshot | undefined { const row = this.db.prepare("SELECT * FROM runs WHERE idempotency_key=?").get(key) as Row | undefined; return row ? this.snapshot(row) : undefined; }
  events(id: string, after = -1): Array<{ seq: number; event: unknown }> { return (this.db.prepare("SELECT seq,event FROM events WHERE run_id=? AND seq>? ORDER BY seq").all(id, after) as Row[]).map((row) => ({ seq: Number(row.seq), event: JSON.parse(String(row.event)) })); }
  cancel(id: string): DurableRunSnapshot | undefined { const row = this.db.prepare("SELECT * FROM runs WHERE id=?").get(id) as Row | undefined; if (!row) return undefined; if (row.state === "queued") this.transition(id, "canceled", "terminal", { error: "canceled" }); return this.get(id); }
  transition(id: string, state: DurableRunState, phase: DurableRunPhase, patch: { result?: unknown; error?: string; runtimeHint?: Record<string, unknown>; elapsedMs?: number } = {}): void {
    const t = this.now(); const row = this.db.prepare("SELECT max(seq) AS seq FROM events WHERE run_id=?").get(id) as Row; const seq = Number(row.seq ?? -1) + 1;
    this.db.exec("BEGIN IMMEDIATE"); try { this.db.prepare("UPDATE runs SET state=?,phase=?,updated_at=?,result=?,error=?,runtime_hint=?,elapsed_ms=? WHERE id=?").run(state, phase, t, patch.result === undefined ? null : JSON.stringify(patch.result), patch.error ?? null, patch.runtimeHint ? JSON.stringify(patch.runtimeHint) : null, patch.elapsedMs ?? null, id); this.db.prepare("INSERT INTO events (run_id,seq,created_at,event) VALUES (?,?,?,?)").run(id, seq, t, JSON.stringify({ type: "state", state, phase })); this.db.exec("COMMIT"); } catch (error) { this.db.exec("ROLLBACK"); throw error; }
    if ((state === "completed" || state === "failed") && patch.elapsedMs !== undefined) this.db.prepare("INSERT INTO eta_samples VALUES (?,?)").run(patch.elapsedMs, t);
  }
  claimNext(): DurableRunSnapshot | undefined {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const active = Number((this.db.prepare("SELECT count(*) AS n FROM runs WHERE state='running'").get() as Row).n);
      if (active >= this.capacity) { this.db.exec("ROLLBACK"); return undefined; }
      const row = this.db.prepare("SELECT * FROM runs WHERE state='queued' ORDER BY created_at,id LIMIT 1").get() as Row | undefined;
      if (!row) { this.db.exec("ROLLBACK"); return undefined; }
      const t = this.now(); const id = String(row.id);
      this.db.prepare("UPDATE runs SET state='running',phase='dispatching',updated_at=? WHERE id=? AND state='queued'").run(t, id);
      const seq = Number((this.db.prepare("SELECT max(seq) AS seq FROM events WHERE run_id=?").get(id) as Row).seq) + 1;
      this.db.prepare("INSERT INTO events (run_id,seq,created_at,event) VALUES (?,?,?,?)").run(id, seq, t, JSON.stringify({ type: "state", state: "running", phase: "dispatching" }));
      this.db.exec("COMMIT"); return this.get(id);
    } catch (error) { this.db.exec("ROLLBACK"); throw error; }
  }
  status(): { active: number; queued: number; capacity: number; backlog: number; roughEtaMs: number } { const count = (state: string) => Number((this.db.prepare("SELECT count(*) AS n FROM runs WHERE state=?").get(state) as Row).n); return { active: count("running"), queued: count("queued"), capacity: this.capacity, backlog: this.backlog, roughEtaMs: this.eta() }; }
  async request(id: string): Promise<DurableRunRequest | undefined> { const row = this.db.prepare("SELECT request_path FROM runs WHERE id=?").get(id) as Row | undefined; if (!row) return undefined; const info = await lstat(String(row.request_path)); if (!info.isFile() || info.isSymbolicLink()) throw new Error("unsafe request path"); return JSON.parse(await readFile(String(row.request_path), "utf8")) as DurableRunRequest; }
  runDirectory(id: string): string { if (!/^[a-zA-Z0-9_-]{1,128}$/.test(id)) throw new Error("invalid run id"); return path.join(this.root, "runs", id); }
}
