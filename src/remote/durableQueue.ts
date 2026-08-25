import { DatabaseSync } from "node:sqlite";
import { constants as fsConstants } from "node:fs";
import { createHash, createHmac, randomUUID } from "node:crypto";
import { chmod, lstat, mkdir, open, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { getOracleHomeDir } from "../oracleHome.js";

export const DURABLE_QUEUE_CAPABILITY_ID = "oracle.remote.durable-queue";
export const DURABLE_QUEUE_CAPABILITY_VERSION = 1;
export const DEFAULT_DURABLE_ACTIVE_CAPACITY = 4;
export const DEFAULT_DURABLE_BACKLOG = 8;
export const DURABLE_ETA_FLOOR_MS = 300_000;
export const MAINTENANCE_DRAIN_CAPABILITY_ID = "oracle.remote.maintenance-drain";
export const MAINTENANCE_DRAIN_CAPABILITY_VERSION = 1;
export const CAPTURE_GRANT_CAPABILITY_ID = "oracle.remote.capture-grant";
export const CAPTURE_GRANT_CAPABILITY_VERSION = 1;
export const CAPTURE_GRANT_TTL_MS = 10 * 60 * 1000;
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
export interface MaintenanceDrainSnapshot {
  schemaVersion: 1;
  drainId: string;
  state: "draining";
  startedAt: string;
  activeRuns: number;
  queuedRuns: number;
}
export interface MaintenanceAdmissionSnapshot {
  state: "open" | "draining";
  drainId?: string;
  startedAt?: string;
}
export type CaptureGrantState =
  | "issued"
  | "reserved"
  | "admitted"
  | "completed"
  | "failed"
  | "revoked"
  | "expired";
export interface CaptureGrantIssue {
  schemaVersion: 1;
  grantId: string;
  tokenType: "Bearer";
  token: string;
  conversationId: string;
  expiresAt: string;
  /** Internal HTTP status hint; omitted from the public response body. */
  replayed?: boolean;
}
export interface CaptureGrantTerminalReceipt {
  state: DurableRunState;
  verified: boolean;
  submissionAttempted: boolean;
  promptSubmitted: boolean;
  artifactCount?: number;
  artifactManifestSha256?: string;
  finalEventSeq?: number;
  failureCode?: string;
}
export interface CaptureGrantReceipt {
  schemaVersion: 1;
  grantId: string;
  drainId: string;
  conversationId: string;
  state: CaptureGrantState;
  createdAt: string;
  expiresAt: string;
  updatedAt: string;
  runId?: string;
  terminal?: CaptureGrantTerminalReceipt;
}
export interface CaptureGrantAuthorization {
  grantId: string;
  drainId: string;
  conversationId: string;
  conversationUrl: string;
  state: CaptureGrantState;
  active: boolean;
  runId?: string;
}
export interface CaptureGrantRun {
  grantId: string;
  drainId: string;
  conversationId: string;
  conversationUrl: string;
}
export interface CaptureGrantAudit {
  artifactManifestSha256: string;
  artifactCount: number;
  submissionAttempted: false;
  promptSubmitted: false;
}
type Row = Record<string, unknown>;
const terminal = new Set<DurableRunState>(["completed", "failed", "canceled", "unknown"]);
const terminalGrantStates = new Set<CaptureGrantState>([
  "completed",
  "failed",
  "revoked",
  "expired",
]);
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
function deriveCaptureGrantToken(params: {
  tokenDerivationKey: string;
  grantId: string;
  drainId: string;
  issueIdempotencyKey: string;
  requestHash: string;
}): string {
  const secret = createHmac("sha256", params.tokenDerivationKey)
    .update("oracle-capture-grant/v1\0")
    .update(params.grantId)
    .update("\0")
    .update(params.drainId)
    .update("\0")
    .update(params.issueIdempotencyKey)
    .update("\0")
    .update(params.requestHash)
    .digest("base64url");
  return `ocg1.${params.grantId}.${secret}`;
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
      `CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY,admission_seq INTEGER NOT NULL UNIQUE,idempotency_key TEXT NOT NULL UNIQUE,request_hash TEXT NOT NULL,request_path TEXT NOT NULL,state TEXT NOT NULL,phase TEXT NOT NULL,seq INTEGER NOT NULL DEFAULT 0,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,runtime_hint TEXT,result TEXT,error TEXT,error_meta TEXT,cancellation TEXT,model TEXT,eta_qualifying INTEGER NOT NULL DEFAULT 0,elapsed_ms INTEGER);
       CREATE TABLE IF NOT EXISTS events(run_id TEXT NOT NULL REFERENCES runs(id),seq INTEGER NOT NULL,created_at INTEGER NOT NULL,event TEXT NOT NULL,PRIMARY KEY(run_id,seq));
       CREATE TABLE IF NOT EXISTS eta_samples(elapsed_ms INTEGER NOT NULL,created_at INTEGER NOT NULL,model TEXT NOT NULL,qualifying INTEGER NOT NULL DEFAULT 1);
       CREATE TABLE IF NOT EXISTS maintenance_state(singleton INTEGER PRIMARY KEY CHECK(singleton=1),state TEXT NOT NULL CHECK(state IN ('open','draining')),drain_id TEXT UNIQUE,started_at INTEGER,idempotency_key TEXT,request_hash TEXT);
       INSERT OR IGNORE INTO maintenance_state(singleton,state) VALUES(1,'open');
       CREATE TABLE IF NOT EXISTS capture_grants(grant_id TEXT PRIMARY KEY,drain_id TEXT NOT NULL,token_sha256 TEXT NOT NULL UNIQUE,conversation_id TEXT NOT NULL,conversation_url TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('issued','reserved','admitted','completed','failed','revoked','expired')),created_at INTEGER NOT NULL,expires_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,issue_idempotency_key TEXT,issue_request_hash TEXT,idempotency_key TEXT,request_hash TEXT,run_id TEXT UNIQUE REFERENCES runs(id),artifact_manifest_sha256 TEXT,artifact_count INTEGER,submission_attempted INTEGER NOT NULL DEFAULT 0,prompt_submitted INTEGER NOT NULL DEFAULT 0,terminal_state TEXT,final_event_seq INTEGER,failure_code TEXT)`,
    );
    const grantColumns = new Set(
      (this.db.prepare("PRAGMA table_info(capture_grants)").all() as Row[]).map((row) =>
        String(row.name),
      ),
    );
    if (!grantColumns.has("issue_idempotency_key"))
      this.db.exec("ALTER TABLE capture_grants ADD COLUMN issue_idempotency_key TEXT");
    if (!grantColumns.has("issue_request_hash"))
      this.db.exec("ALTER TABLE capture_grants ADD COLUMN issue_request_hash TEXT");
    this.db.exec(
      "CREATE UNIQUE INDEX IF NOT EXISTS capture_grants_issue_key ON capture_grants(drain_id,issue_idempotency_key) WHERE issue_idempotency_key IS NOT NULL",
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
    await safeDescendant(root, `${dbPath}-shm`, "SHM");
    const db = new DatabaseSync(dbPath);
    await chmod(dbPath, 0o600);
    for (const suffix of ["-wal", "-shm"]) {
      const p = `${dbPath}${suffix}`;
      if (await lstat(p).catch(() => undefined)) {
        await safeDescendant(root, p, suffix.slice(1));
        await chmod(p, 0o600);
      }
    }
    const store = new DurableQueueStore(db, root, o);
    for (const suffix of ["-wal", "-shm"]) {
      const p = `${dbPath}${suffix}`;
      if (await lstat(p).catch(() => undefined)) await chmod(p, 0o600);
    }
    return store;
  }
  close(): void {
    if (this.db.isOpen) this.db.close();
  }
  private append(id: string, t: number, event: unknown): number {
    const r = this.db.prepare("SELECT seq FROM runs WHERE id=?").get(id) as Row;
    const seq = Number(r.seq);
    this.db
      .prepare("INSERT INTO events(run_id,seq,created_at,event) VALUES(?,?,?,?)")
      .run(id, seq, t, JSON.stringify(event));
    this.db.prepare("UPDATE runs SET seq=? WHERE id=?").run(seq + 1, id);
    return seq;
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
        const finalEventSeq = this.append(id, t, {
          type: "state",
          state: "unknown",
          phase: "terminal",
        });
        this.db
          .prepare(
            "UPDATE capture_grants SET state='failed',updated_at=?,terminal_state='unknown',final_event_seq=?,failure_code=COALESCE(failure_code,'restart_interrupted') WHERE run_id=? AND state IN ('reserved','admitted')",
          )
          .run(t, finalEventSeq, id);
        this.db.exec("COMMIT");
      } catch (e) {
        this.db.exec("ROLLBACK");
        throw e;
      }
    }
  }
  private stageRequest(
    id: string,
    request: DurableRunRequest,
  ): Promise<{ directory: string; requestPath: string }> {
    return (async () => {
      const directory = path.join(this.root, "runs", id);
      await privateDirectory(directory);
      const requestPath = path.join(directory, "request.json");
      const temporaryPath = `${requestPath}.part-${randomUUID()}`;
      await writeFile(temporaryPath, JSON.stringify(request), { mode: 0o600 });
      await chmod(temporaryPath, 0o600);
      const file = await open(temporaryPath, "r");
      await file.sync();
      await file.close();
      await rename(temporaryPath, requestPath);
      await chmod(requestPath, 0o600);
      const parent = await open(directory, "r");
      await parent.sync();
      await parent.close();
      return { directory, requestPath };
    })();
  }
  private drainSnapshot(row: Row): MaintenanceDrainSnapshot {
    const queue = this.status();
    return {
      schemaVersion: 1,
      drainId: String(row.drain_id),
      state: "draining",
      startedAt: new Date(Number(row.started_at)).toISOString(),
      activeRuns: queue.active,
      queuedRuns: queue.queued,
    };
  }
  private captureGrantReceipt(row: Row): CaptureGrantReceipt {
    const state = row.state as CaptureGrantState;
    const terminalReceipt = terminalGrantStates.has(state)
      ? {
          state: (row.terminal_state ??
            (state === "revoked" ? "canceled" : "failed")) as DurableRunState,
          verified: state === "completed",
          submissionAttempted: Number(row.submission_attempted) === 1,
          promptSubmitted: Number(row.prompt_submitted) === 1,
          ...(row.artifact_count === null || row.artifact_count === undefined
            ? {}
            : { artifactCount: Number(row.artifact_count) }),
          ...(row.artifact_manifest_sha256
            ? { artifactManifestSha256: String(row.artifact_manifest_sha256) }
            : {}),
          ...(row.final_event_seq === null || row.final_event_seq === undefined
            ? {}
            : { finalEventSeq: Number(row.final_event_seq) }),
          ...(row.failure_code ? { failureCode: String(row.failure_code) } : {}),
        }
      : undefined;
    return {
      schemaVersion: 1,
      grantId: String(row.grant_id),
      drainId: String(row.drain_id),
      conversationId: String(row.conversation_id),
      state,
      createdAt: new Date(Number(row.created_at)).toISOString(),
      expiresAt: new Date(Number(row.expires_at)).toISOString(),
      updatedAt: new Date(Number(row.updated_at)).toISOString(),
      ...(row.run_id ? { runId: String(row.run_id) } : {}),
      ...(terminalReceipt ? { terminal: terminalReceipt } : {}),
    };
  }
  private expireIssuedCaptureGrants(t = this.now()): void {
    this.db
      .prepare(
        "UPDATE capture_grants SET state='expired',updated_at=?,terminal_state='failed',failure_code='capture_grant_expired' WHERE state='issued' AND expires_at<=?",
      )
      .run(t, t);
  }
  beginDrainIfIdle(key: string, mode: "require-idle"): MaintenanceDrainSnapshot {
    if (!key || key.length > 512) throw new Error("stable Idempotency-Key is required");
    if (mode !== "require-idle") throw new Error("invalid_maintenance_request");
    const requestHash = digest({ mode });
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const current = this.db
        .prepare("SELECT * FROM maintenance_state WHERE singleton=1")
        .get() as Row;
      if (current.state === "draining") {
        if (current.idempotency_key === key && current.request_hash === requestHash) {
          this.db.exec("COMMIT");
          return this.drainSnapshot(current);
        }
        throw new Error("maintenance_drain_active");
      }
      const counts = this.status();
      if (counts.active !== 0 || counts.queued !== 0) throw new Error("bridge_busy");
      const drainId = randomUUID();
      const startedAt = this.now();
      this.db
        .prepare(
          "UPDATE maintenance_state SET state='draining',drain_id=?,started_at=?,idempotency_key=?,request_hash=? WHERE singleton=1",
        )
        .run(drainId, startedAt, key, requestHash);
      const row = this.db.prepare("SELECT * FROM maintenance_state WHERE singleton=1").get() as Row;
      this.db.exec("COMMIT");
      return this.drainSnapshot(row);
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
  getDrain(drainId: string): MaintenanceDrainSnapshot | undefined {
    const row = this.db
      .prepare(
        "SELECT * FROM maintenance_state WHERE singleton=1 AND state='draining' AND drain_id=?",
      )
      .get(drainId) as Row | undefined;
    return row ? this.drainSnapshot(row) : undefined;
  }
  admission(): MaintenanceAdmissionSnapshot {
    const row = this.db.prepare("SELECT * FROM maintenance_state WHERE singleton=1").get() as Row;
    return row.state === "draining"
      ? {
          state: "draining",
          drainId: String(row.drain_id),
          startedAt: new Date(Number(row.started_at)).toISOString(),
        }
      : { state: "open" };
  }
  issueCaptureGrant(
    drainId: string,
    conversationId: string,
    conversationUrl: string,
    issueIdempotencyKey: string,
    tokenDerivationKey: string,
  ): CaptureGrantIssue {
    if (!issueIdempotencyKey || issueIdempotencyKey.length > 512)
      throw new Error("stable Idempotency-Key is required");
    if (!tokenDerivationKey) throw new Error("capture grant token authority is unavailable");
    const requestHash = digest({ conversationId, conversationUrl });
    const createdAt = this.now();
    const expiresAt = createdAt + CAPTURE_GRANT_TTL_MS;
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const drain = this.db
        .prepare("SELECT * FROM maintenance_state WHERE singleton=1")
        .get() as Row;
      if (drain.state !== "draining" || drain.drain_id !== drainId)
        throw new Error("maintenance_drain_not_found");
      const prior = this.db
        .prepare("SELECT * FROM capture_grants WHERE drain_id=? AND issue_idempotency_key=?")
        .get(drainId, issueIdempotencyKey) as Row | undefined;
      if (prior) {
        if (prior.issue_request_hash !== requestHash)
          throw new Error("capture_grant_issue_conflict");
        const priorToken = deriveCaptureGrantToken({
          tokenDerivationKey,
          grantId: String(prior.grant_id),
          drainId,
          issueIdempotencyKey,
          requestHash,
        });
        if (createHash("sha256").update(priorToken).digest("hex") !== prior.token_sha256)
          throw new Error("capture_grant_token_mismatch");
        this.db.exec("COMMIT");
        return {
          schemaVersion: 1,
          grantId: String(prior.grant_id),
          tokenType: "Bearer",
          token: priorToken,
          conversationId: String(prior.conversation_id),
          expiresAt: new Date(Number(prior.expires_at)).toISOString(),
          replayed: true,
        };
      }
      const grantId = randomUUID();
      const token = deriveCaptureGrantToken({
        tokenDerivationKey,
        grantId,
        drainId,
        issueIdempotencyKey,
        requestHash,
      });
      const tokenSha256 = createHash("sha256").update(token).digest("hex");
      this.db
        .prepare(
          "INSERT INTO capture_grants(grant_id,drain_id,token_sha256,conversation_id,conversation_url,state,created_at,expires_at,updated_at,issue_idempotency_key,issue_request_hash) VALUES(?,?,?,?,?,'issued',?,?,?,?,?)",
        )
        .run(
          grantId,
          drainId,
          tokenSha256,
          conversationId,
          conversationUrl,
          createdAt,
          expiresAt,
          createdAt,
          issueIdempotencyKey,
          requestHash,
        );
      this.db.exec("COMMIT");
      return {
        schemaVersion: 1,
        grantId,
        tokenType: "Bearer",
        token,
        conversationId,
        expiresAt: new Date(expiresAt).toISOString(),
        replayed: false,
      };
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
  getCaptureGrant(grantId: string): CaptureGrantReceipt | undefined {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.expireIssuedCaptureGrants();
      const row = this.db.prepare("SELECT * FROM capture_grants WHERE grant_id=?").get(grantId) as
        | Row
        | undefined;
      this.db.exec("COMMIT");
      return row ? this.captureGrantReceipt(row) : undefined;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
  authorizeCaptureGrant(token: string): CaptureGrantAuthorization | undefined {
    if (!token) return;
    const tokenSha256 = createHash("sha256").update(token).digest("hex");
    const row = this.db
      .prepare(
        "SELECT capture_grants.*,maintenance_state.state maintenance_state,maintenance_state.drain_id active_drain_id FROM capture_grants CROSS JOIN maintenance_state WHERE maintenance_state.singleton=1 AND capture_grants.token_sha256=?",
      )
      .get(tokenSha256) as Row | undefined;
    if (!row || Number(row.expires_at) <= this.now() || row.state === "expired") return;
    return {
      grantId: String(row.grant_id),
      drainId: String(row.drain_id),
      conversationId: String(row.conversation_id),
      conversationUrl: String(row.conversation_url),
      state: row.state as CaptureGrantState,
      active: row.maintenance_state === "draining" && row.active_drain_id === row.drain_id,
      ...(row.run_id ? { runId: String(row.run_id) } : {}),
    };
  }
  captureGrantForRun(runId: string): CaptureGrantRun | undefined {
    const row = this.db.prepare("SELECT * FROM capture_grants WHERE run_id=?").get(runId) as
      | Row
      | undefined;
    return row
      ? {
          grantId: String(row.grant_id),
          drainId: String(row.drain_id),
          conversationId: String(row.conversation_id),
          conversationUrl: String(row.conversation_url),
        }
      : undefined;
  }
  releaseDrain(drainId: string): { schemaVersion: 1; drainId: string; state: "open" } {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const drain = this.db
        .prepare("SELECT * FROM maintenance_state WHERE singleton=1")
        .get() as Row;
      if (drain.state !== "draining" || drain.drain_id !== drainId)
        throw new Error("maintenance_drain_not_found");
      this.expireIssuedCaptureGrants();
      const counts = this.status();
      if (counts.active !== 0 || counts.queued !== 0) throw new Error("bridge_busy");
      // A grant can be issued before its HTTP response reaches the controller.
      // Once the bridge is otherwise idle, revoking unused grants makes release
      // retryable instead of fencing admission until the grant TTL expires.
      this.db
        .prepare(
          "UPDATE capture_grants SET state='revoked',updated_at=?,terminal_state='canceled',failure_code='maintenance_drain_released' WHERE drain_id=? AND state='issued' AND run_id IS NULL",
        )
        .run(this.now(), drainId);
      const pending = Number(
        (
          this.db
            .prepare(
              "SELECT count(*) n FROM capture_grants WHERE drain_id=? AND state IN ('reserved','admitted')",
            )
            .get(drainId) as Row
        ).n,
      );
      if (pending !== 0) throw new Error("capture_grants_pending");
      this.db
        .prepare(
          "UPDATE maintenance_state SET state='open',drain_id=NULL,started_at=NULL,idempotency_key=NULL,request_hash=NULL WHERE singleton=1",
        )
        .run();
      this.db.exec("COMMIT");
      return { schemaVersion: 1, drainId, state: "open" };
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
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
      t = this.now();
    const { directory: dir, requestPath: rp } = await this.stageRequest(id, request);
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
      const maintenance = this.db
        .prepare("SELECT state FROM maintenance_state WHERE singleton=1")
        .get() as Row;
      if (maintenance.state === "draining") throw new Error("admission_draining");
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
  async submitGrantedCapture(
    token: string,
    key: string,
    request: DurableRunRequest,
  ): Promise<DurableRunSnapshot> {
    if (!key || key.length > 512) throw new Error("stable Idempotency-Key is required");
    const tokenSha256 = createHash("sha256").update(token).digest("hex");
    const hash = digest(request);
    const id = randomUUID();
    const t = this.now();
    const { directory, requestPath } = await this.stageRequest(id, request);
    let committed = false;
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const grant = this.db
        .prepare("SELECT * FROM capture_grants WHERE token_sha256=?")
        .get(tokenSha256) as Row | undefined;
      if (!grant || Number(grant.expires_at) <= t || grant.state === "expired")
        throw new Error("capture_grant_invalid");
      if (grant.run_id) {
        const prior = this.db.prepare("SELECT * FROM runs WHERE id=?").get(String(grant.run_id)) as
          | Row
          | undefined;
        if (
          prior &&
          grant.idempotency_key === key &&
          grant.request_hash === hash &&
          prior.idempotency_key === key &&
          prior.request_hash === hash
        ) {
          this.db.exec("ROLLBACK");
          await rm(directory, { recursive: true, force: true });
          return this.snapshot(prior);
        }
        throw new Error("capture_grant_consumed");
      }
      if (grant.state !== "issued") throw new Error("capture_grant_consumed");
      const maintenance = this.db
        .prepare("SELECT * FROM maintenance_state WHERE singleton=1")
        .get() as Row;
      if (maintenance.state !== "draining" || maintenance.drain_id !== grant.drain_id)
        throw new Error("capture_grant_not_active");
      const existingKey = this.db.prepare("SELECT * FROM runs WHERE idempotency_key=?").get(key) as
        | Row
        | undefined;
      if (existingKey) throw new Error("idempotency key conflicts with an existing request");
      const total = Number(
        (
          this.db
            .prepare(
              "SELECT count(*) n FROM runs WHERE state NOT IN ('completed','failed','canceled','unknown')",
            )
            .get() as Row
        ).n,
      );
      if (total >= this.capacity + this.backlog) throw new Error("queue_full");
      this.db
        .prepare(
          "INSERT INTO runs(id,admission_seq,idempotency_key,request_hash,request_path,state,phase,created_at,updated_at) VALUES(?,COALESCE((SELECT max(admission_seq)+1 FROM runs),1),?,?,?,?,?,?,?)",
        )
        .run(id, key, hash, requestPath, "queued", "accepted", t, t);
      this.append(id, t, { type: "accepted" });
      this.append(id, t, {
        type: "maintenance-capture-authorized",
        grantId: String(grant.grant_id),
        drainId: String(grant.drain_id),
        conversationId: String(grant.conversation_id),
      });
      this.db
        .prepare(
          "UPDATE capture_grants SET state='admitted',updated_at=?,idempotency_key=?,request_hash=?,run_id=? WHERE grant_id=? AND state='issued' AND run_id IS NULL",
        )
        .run(t, key, hash, id, String(grant.grant_id));
      this.db.exec("COMMIT");
      committed = true;
      return this.snapshot(this.db.prepare("SELECT * FROM runs WHERE id=?").get(id) as Row);
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
      } catch {}
      if (!committed) await rm(directory, { recursive: true, force: true }).catch(() => undefined);
      throw error;
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
  appendEvent(id: string, event: unknown): number {
    const t = this.now();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const seq = this.append(id, t, event);
      this.db
        .prepare(
          "UPDATE capture_grants SET updated_at=?,final_event_seq=? WHERE run_id=? AND state IN ('completed','failed','revoked','expired')",
        )
        .run(t, seq, id);
      this.db.exec("COMMIT");
      return seq;
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }
  recordCaptureEvidence(
    id: string,
    evidence: { submissionAttempted?: boolean; promptSubmitted?: boolean },
  ): void {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const grant = this.db.prepare("SELECT * FROM capture_grants WHERE run_id=?").get(id) as
        | Row
        | undefined;
      if (!grant) throw new Error("capture grant not found for run");
      this.db
        .prepare(
          "UPDATE capture_grants SET updated_at=?,submission_attempted=MAX(submission_attempted,?),prompt_submitted=MAX(prompt_submitted,?) WHERE run_id=?",
        )
        .run(
          this.now(),
          evidence.submissionAttempted === true ? 1 : 0,
          evidence.promptSubmitted === true ? 1 : 0,
          id,
        );
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
  recordCaptureViolation(id: string, code: string): number {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const grant = this.db.prepare("SELECT * FROM capture_grants WHERE run_id=?").get(id) as
        | Row
        | undefined;
      if (!grant) throw new Error("capture grant not found for run");
      const t = this.now();
      const seq = this.append(id, t, { type: "maintenance-capture-violation", code });
      this.db
        .prepare(
          "UPDATE capture_grants SET updated_at=?,failure_code=?,final_event_seq=CASE WHEN state IN ('completed','failed','revoked','expired') THEN ? ELSE final_event_seq END WHERE run_id=?",
        )
        .run(t, code, seq, id);
      this.db.exec("COMMIT");
      return seq;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
  cancel(id: string): DurableRunSnapshot | undefined {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const r = this.db.prepare("SELECT * FROM runs WHERE id=?").get(id) as Row | undefined;
      if (!r) {
        this.db.exec("ROLLBACK");
        return;
      }
      if (terminal.has(r.state as DurableRunState)) {
        this.db.exec("COMMIT");
        return this.get(id);
      }
      const t = this.now();
      const preSubmit =
        r.state === "queued" ||
        (r.state === "running" &&
          ["accepted", "dispatching", "browser_attached"].includes(String(r.phase)));
      const out = preSubmit ? "canceled" : "unknown";
      (
        this.db.prepare(
          "UPDATE runs SET cancellation=?,updated_at=?,state=?,phase=? WHERE id=?",
        ) as any
      ).run(
        JSON.stringify({ requestedAt: new Date(t).toISOString(), outcome: out }),
        t,
        out,
        "terminal",
        id,
      );
      const finalEventSeq = this.append(id, t, { type: "cancellation", outcome: out });
      this.db
        .prepare(
          "UPDATE capture_grants SET state='revoked',updated_at=?,terminal_state=?,final_event_seq=?,failure_code='capture_grant_canceled' WHERE run_id=? AND state IN ('reserved','admitted')",
        )
        .run(t, out, finalEventSeq, id);
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
      captureAudit?: CaptureGrantAudit;
    } = {},
  ): void {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const o = this.db.prepare("SELECT * FROM runs WHERE id=?").get(id) as Row | undefined;
      if (!o) throw new Error("unknown run");
      if (terminal.has(o.state as DurableRunState) && o.state === state) {
        this.db.exec("COMMIT");
        return;
      }
      if (terminal.has(o.state as DurableRunState))
        throw new Error("terminal run cannot transition");
      const t = this.now(),
        hint =
          p.runtimeHint === undefined
            ? o.runtime_hint
              ? JSON.parse(String(o.runtime_hint))
              : null
            : { ...(o.runtime_hint ? JSON.parse(String(o.runtime_hint)) : {}), ...p.runtimeHint };
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
      const finalEventSeq = this.append(id, t, { type: "state", state, phase });
      const captureGrant = this.db
        .prepare("SELECT * FROM capture_grants WHERE run_id=?")
        .get(id) as Row | undefined;
      if (captureGrant && terminal.has(state)) {
        if (state === "completed" && !p.captureAudit)
          throw new Error("capture grant completion requires verified bridge evidence");
        const grantState: CaptureGrantState =
          state === "completed" ? "completed" : state === "canceled" ? "revoked" : "failed";
        const failureCode =
          state === "completed"
            ? null
            : (p.errorMetadata?.code ??
              (captureGrant.failure_code ? String(captureGrant.failure_code) : "capture_failed"));
        this.db
          .prepare(
            "UPDATE capture_grants SET state=?,updated_at=?,terminal_state=?,artifact_manifest_sha256=?,artifact_count=?,submission_attempted=MAX(submission_attempted,?),prompt_submitted=MAX(prompt_submitted,?),final_event_seq=?,failure_code=? WHERE run_id=?",
          )
          .run(
            grantState,
            t,
            state,
            p.captureAudit?.artifactManifestSha256 ?? null,
            p.captureAudit?.artifactCount ?? null,
            0,
            0,
            finalEventSeq,
            failureCode,
            id,
          );
      }
      if (
        terminal.has(state) &&
        p.elapsedMs !== undefined &&
        state === "completed" &&
        /pro/i.test(p.model ?? String(o.model ?? "")) &&
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
