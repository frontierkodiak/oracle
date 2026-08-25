import { createHash, randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { chmod, mkdir, open, readFile, rename, rm, lstat } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";
import path from "node:path";
import { getOracleHomeDir } from "./oracleHome.js";
import { extractStableConversationIdFromUrl } from "./browser/conversationUrl.js";

export const TRANSCRIPT_LEDGER_SCHEMA = "oracle.transcript-ledger/v1";
export const TRANSCRIPT_LEDGER_NORMALIZATION = "oracle.transcript-ledger-normalized-turns/v1";
export const DEFAULT_LEDGER_DIR_NAME = "transcript-ledger";

export type LedgerObservationStatus = "captured" | "failed" | "challenged" | "auth-unavailable";

export interface TranscriptLedgerOptions {
  root?: string;
}

export interface IngestPairInput {
  provider: string;
  profileId: string;
  conversationId?: string;
  canonicalUrl?: string;
  rawPath: string;
  evidencePath: string;
  capturedAt?: string;
  captureMethod?: string;
}

export interface FailedObservationInput {
  provider: string;
  profileId: string;
  conversationId: string;
  canonicalUrl?: string;
  status: Exclude<LedgerObservationStatus, "captured">;
  errorCode: string;
  errorMessage?: string;
  capturedAt?: string;
}

export interface LedgerIngestResult {
  conversationKey: string;
  observationId: string;
  revisionId: string;
  deduplicated: boolean;
  rawSha256: string;
  evidenceSha256: string;
  normalizedSequenceSha256: string;
  warning?: LedgerWarning;
}

export interface LedgerWarning {
  code: "transcript-ledger-ingest-failed";
  severity: "warning";
  message: string;
}

export interface LedgerListRow {
  conversationKey: string;
  provider: string;
  profileId: string;
  conversationId: string;
  canonicalUrl: string | null;
  state: string;
  revisionCount: number;
  observationCount: number;
  latestRevisionAt: string | null;
}

type RawNode = {
  id?: unknown;
  parent?: unknown;
  message?: Record<string, unknown> | null;
};

type NormalizedTurn = {
  ordinal: number;
  nodeId: string;
  parentId: string | null;
  role: string;
  contentType: string;
  body: string;
  bodySha256: string;
  bodyBytes: number;
  attachments: unknown[];
};

function hashBytes(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function hashText(value: string): string {
  return hashBytes(Buffer.from(value, "utf8"));
}

function stableJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableJson(record[key])}`)
    .join(",")}}`;
}

function now(): string {
  return new Date().toISOString();
}

function safeId(value: string, label: string): string {
  const trimmed = value.trim();
  if (!trimmed || trimmed.length > 512 || /[\u0000\n\r]/.test(trimmed)) {
    throw new Error(`${label} is empty or invalid`);
  }
  return trimmed;
}

export function resolveTranscriptLedgerRoot(root?: string): string {
  return path.resolve(
    root ??
      process.env.ORACLE_TRANSCRIPT_LEDGER_DIR ??
      path.join(getOracleHomeDir(), DEFAULT_LEDGER_DIR_NAME),
  );
}

function checkMode(mode: number, expected: number, target: string): void {
  if ((mode & 0o777) !== expected) throw new Error(`ledger path has unsafe permissions: ${target}`);
}

async function ensurePrivateDirectory(target: string): Promise<void> {
  const absolute = path.resolve(target);
  const missing: string[] = [];
  let current = absolute;
  while (true) {
    try {
      const entry = await lstat(current);
      if (entry.isSymbolicLink())
        throw new Error(`ledger path may not contain symlinks: ${current}`);
      if (!entry.isDirectory()) throw new Error(`ledger path is not a directory: ${current}`);
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      missing.push(current);
      const parent = path.dirname(current);
      if (parent === current)
        throw new Error(`unable to establish private ledger path: ${absolute}`);
      current = parent;
    }
  }
  for (let index = missing.length - 1; index >= 0; index -= 1) {
    const dir = missing[index];
    if (!dir) continue;
    await mkdir(dir, { mode: 0o700 });
    await chmod(dir, 0o700);
    checkMode((await lstat(dir)).mode, 0o700, dir);
  }
  // Only ledger-owned directories are normalized; never chmod an ancestor such as /Users.
  if (missing.length === 0 || missing[missing.length - 1] === absolute) {
    await chmod(absolute, 0o700);
    checkMode((await lstat(absolute)).mode, 0o700, absolute);
  }
}

async function ensurePrivateFile(target: string): Promise<void> {
  const entry = await lstat(target);
  if (entry.isSymbolicLink() || !entry.isFile())
    throw new Error(`ledger file is not a regular file: ${target}`);
  await chmod(target, 0o600);
  checkMode((await lstat(target)).mode, 0o600, target);
}

async function ensureRoot(root: string): Promise<void> {
  await ensurePrivateDirectory(root);
  await ensurePrivateDirectory(path.join(root, "objects", "sha256"));
  const index = path.join(root, "index.sqlite");
  try {
    await ensurePrivateFile(index);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    const handle = await open(index, "wx", 0o600);
    await handle.close();
  }
}

function objectPath(root: string, digest: string): string {
  return path.join(root, "objects", "sha256", digest.slice(0, 2), digest);
}

async function writeObject(
  root: string,
  bytes: Uint8Array,
  digest: string,
): Promise<{ path: string; created: boolean }> {
  const target = objectPath(root, digest);
  const parent = path.dirname(target);
  await ensurePrivateDirectory(parent);
  try {
    await ensurePrivateFile(target);
    const existing = await readFile(target);
    if (hashBytes(existing) !== digest)
      throw new Error(`content-addressed object mismatch: ${target}`);
    return { path: target, created: false };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const temp = path.join(parent, `.${digest}.${randomUUID()}.tmp`);
  const handle = await open(
    temp,
    fsConstants.O_WRONLY | fsConstants.O_CREAT | fsConstants.O_EXCL | fsConstants.O_NOFOLLOW,
    0o600,
  );
  try {
    await handle.writeFile(bytes);
    await handle.sync();
    await handle.close();
    await chmod(temp, 0o600);
    await rename(temp, target);
    return { path: target, created: true };
  } catch (error) {
    await handle.close().catch(() => undefined);
    await rm(temp, { force: true }).catch(() => undefined);
    throw error;
  }
}

async function readArtifactBytes(target: string): Promise<Buffer> {
  const entry = await lstat(target);
  if (entry.isSymbolicLink() || !entry.isFile()) {
    throw new Error(`capture artifact must be a regular non-symlink file: ${target}`);
  }
  return readFile(target);
}

function parseConversation(
  raw: Record<string, unknown>,
  supplied?: string,
): {
  id: string;
  url: string | null;
  currentNode: string | null;
  mapping: Record<string, RawNode>;
} {
  const id = safeId(supplied ?? String(raw.conversation_id ?? raw.id ?? ""), "conversation id");
  const url = typeof raw.conversation_url === "string" ? raw.conversation_url : null;
  const mapping =
    raw.mapping && typeof raw.mapping === "object" && !Array.isArray(raw.mapping)
      ? (raw.mapping as Record<string, RawNode>)
      : {};
  const current = typeof raw.current_node === "string" ? raw.current_node : null;
  return { id, url, currentNode: current, mapping };
}

function contentBody(message: Record<string, unknown>): { type: string; body: string } {
  const content = message.content;
  const contentType =
    typeof content === "object" && content !== null && !Array.isArray(content)
      ? String((content as Record<string, unknown>).content_type ?? "")
      : "text";
  const parts =
    typeof content === "object" && content !== null && !Array.isArray(content)
      ? (content as Record<string, unknown>).parts
      : undefined;
  if (Array.isArray(parts)) {
    const text = parts
      .map((part) => (typeof part === "string" ? part : stableJson(part)))
      .join("\n\n");
    return { type: contentType || "text", body: text };
  }
  if (typeof content === "string") return { type: contentType || "text", body: content };
  return { type: contentType || "unknown", body: stableJson(content ?? "") };
}

function selectedTurns(parsed: ReturnType<typeof parseConversation>): NormalizedTurn[] {
  const chain: string[] = [];
  const seen = new Set<string>();
  let cursor = parsed.currentNode;
  while (cursor && !seen.has(cursor)) {
    seen.add(cursor);
    chain.push(cursor);
    const parent = parsed.mapping[cursor]?.parent;
    cursor = typeof parent === "string" ? parent : null;
  }
  chain.reverse();
  return chain.flatMap((nodeId, ordinal) => {
    const node = parsed.mapping[nodeId];
    const message = node?.message;
    if (!message || typeof message !== "object") return [];
    const role =
      typeof message.author === "object" && message.author !== null
        ? String((message.author as Record<string, unknown>).role ?? "unknown")
        : "unknown";
    const body = contentBody(message);
    const text = body.body;
    const attachments = Array.isArray(
      message.metadata && typeof message.metadata === "object"
        ? (message.metadata as Record<string, unknown>).attachments
        : undefined,
    )
      ? ((message.metadata as Record<string, unknown>).attachments as unknown[])
      : [];
    return [
      {
        ordinal,
        nodeId,
        parentId: typeof node.parent === "string" ? node.parent : null,
        role,
        contentType: body.type,
        body: text,
        bodySha256: hashText(text),
        bodyBytes: Buffer.byteLength(text),
        attachments,
      },
    ];
  });
}

function conversationKey(provider: string, profileId: string, conversationId: string): string {
  return hashText(
    `${safeId(provider, "provider")}\u0000${safeId(profileId, "profile id")}\u0000${safeId(conversationId, "conversation id")}`,
  );
}

function createDb(indexPath: string): DatabaseSync {
  const db = new DatabaseSync(indexPath);
  db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;");
  db.exec(`
    CREATE TABLE IF NOT EXISTS ledger_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS objects (sha256 TEXT PRIMARY KEY, kind TEXT NOT NULL, bytes INTEGER NOT NULL, path TEXT NOT NULL, created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS conversations (
      conversation_key TEXT PRIMARY KEY, provider TEXT NOT NULL, profile_id TEXT NOT NULL,
      conversation_id TEXT NOT NULL, canonical_url TEXT, state TEXT NOT NULL DEFAULT 'unseen',
      latest_revision_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
      UNIQUE(provider, profile_id, conversation_id)
    );
    CREATE TABLE IF NOT EXISTS revisions (
      revision_id TEXT PRIMARY KEY, conversation_key TEXT NOT NULL REFERENCES conversations(conversation_key),
      normalized_sequence_sha256 TEXT NOT NULL, raw_sha256 TEXT NOT NULL, evidence_sha256 TEXT NOT NULL,
      current_node_id TEXT, turn_count INTEGER NOT NULL, normalization_version TEXT NOT NULL,
      captured_at TEXT NOT NULL, UNIQUE(conversation_key, normalized_sequence_sha256)
    );
    CREATE TABLE IF NOT EXISTS turns (
      revision_id TEXT NOT NULL REFERENCES revisions(revision_id), ordinal INTEGER NOT NULL, node_id TEXT NOT NULL,
      parent_id TEXT, role TEXT NOT NULL, content_type TEXT NOT NULL, body_sha256 TEXT NOT NULL,
      body_bytes INTEGER NOT NULL, attachments_json TEXT NOT NULL, PRIMARY KEY(revision_id, ordinal)
    );
    CREATE TABLE IF NOT EXISTS observations (
      observation_id TEXT PRIMARY KEY, conversation_key TEXT NOT NULL REFERENCES conversations(conversation_key),
      captured_at TEXT NOT NULL, status TEXT NOT NULL, raw_sha256 TEXT, evidence_sha256 TEXT,
      normalized_sequence_sha256 TEXT, revision_id TEXT, source_url TEXT, capture_method TEXT,
      error_code TEXT, error_message TEXT
    );
    CREATE TABLE IF NOT EXISTS watches (
      watch_id TEXT PRIMARY KEY, provider TEXT NOT NULL, profile_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
      canonical_url TEXT, enabled INTEGER NOT NULL DEFAULT 1, interval_seconds INTEGER,
      last_attempt_at TEXT, last_success_at TEXT, last_observation_id TEXT, next_due_at TEXT,
      last_error_code TEXT, UNIQUE(provider, profile_id, conversation_id)
    );
    CREATE INDEX IF NOT EXISTS observations_by_conversation ON observations(conversation_key, captured_at);
    CREATE INDEX IF NOT EXISTS revisions_by_conversation ON revisions(conversation_key, captured_at);
  `);
  db.prepare("INSERT OR IGNORE INTO ledger_meta(key,value) VALUES('schema', ?)").run(
    TRANSCRIPT_LEDGER_SCHEMA,
  );
  return db;
}

export class TranscriptLedger {
  readonly root: string;
  private db?: DatabaseSync;

  private constructor(root: string) {
    this.root = root;
  }

  static async open(options: TranscriptLedgerOptions = {}): Promise<TranscriptLedger> {
    const root = resolveTranscriptLedgerRoot(options.root);
    await ensureRoot(root);
    const ledger = new TranscriptLedger(root);
    ledger.db = createDb(path.join(root, "index.sqlite"));
    await ensurePrivateFile(path.join(root, "index.sqlite-wal")).catch((error) => {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    });
    return ledger;
  }

  close(): void {
    this.db?.close();
    this.db = undefined;
  }

  private get database(): DatabaseSync {
    if (!this.db) throw new Error("transcript ledger is closed");
    return this.db;
  }

  async ingestPair(input: IngestPairInput): Promise<LedgerIngestResult> {
    const rawBytes = await readArtifactBytes(input.rawPath);
    const evidenceBytes = await readArtifactBytes(input.evidencePath);
    const raw = JSON.parse(rawBytes.toString("utf8")) as Record<string, unknown>;
    const parsed = parseConversation(raw, input.conversationId);
    const evidence = JSON.parse(evidenceBytes.toString("utf8")) as Record<string, unknown>;
    if (evidence.schema && evidence.schema !== "oracle.provider-native-capture-evidence/v1") {
      throw new Error("unsupported provider evidence schema");
    }
    const rawSha256 = hashBytes(rawBytes);
    const evidenceSha256 = hashBytes(evidenceBytes);
    if (typeof evidence.conversation_id === "string" && evidence.conversation_id !== parsed.id) {
      throw new Error("raw and evidence conversation ids do not match");
    }
    const materialized = evidence.materialized_document;
    if (
      materialized &&
      typeof materialized === "object" &&
      typeof (materialized as Record<string, unknown>).sha256 === "string" &&
      (materialized as Record<string, unknown>).sha256 !== rawSha256
    ) {
      throw new Error("evidence does not describe the supplied raw bytes");
    }
    const turns = selectedTurns(parsed);
    const sequence = turns.map(
      ({ ordinal, nodeId, parentId, role, contentType, bodySha256, bodyBytes, attachments }) => ({
        ordinal,
        nodeId,
        parentId,
        role,
        contentType,
        bodySha256,
        bodyBytes,
        attachments,
      }),
    );
    const normalizedSequenceSha256 = hashText(stableJson(sequence));
    const key = conversationKey(input.provider, input.profileId, parsed.id);
    const capturedAt = input.capturedAt ?? now();
    const revisionId = hashText(`${key}\u0000${normalizedSequenceSha256}`);
    const observationId = randomUUID();
    const rawObject = await writeObject(this.root, rawBytes, rawSha256);
    const evidenceObject = await writeObject(this.root, evidenceBytes, evidenceSha256);
    const db = this.database;
    let committed = false;
    try {
      db.exec("BEGIN IMMEDIATE");
      db.prepare(`INSERT INTO conversations(conversation_key,provider,profile_id,conversation_id,canonical_url,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(conversation_key) DO UPDATE SET canonical_url=COALESCE(excluded.canonical_url,conversations.canonical_url), state='captured', updated_at=excluded.updated_at`).run(
        key,
        input.provider,
        input.profileId,
        parsed.id,
        input.canonicalUrl ?? parsed.url,
        "captured",
        capturedAt,
        capturedAt,
      );
      db.prepare(
        "INSERT OR IGNORE INTO objects(sha256,kind,bytes,path,created_at) VALUES(?,?,?,?,?)",
      ).run(
        rawSha256,
        "raw-provider-json",
        rawBytes.byteLength,
        path.relative(this.root, rawObject.path),
        capturedAt,
      );
      db.prepare(
        "INSERT OR IGNORE INTO objects(sha256,kind,bytes,path,created_at) VALUES(?,?,?,?,?)",
      ).run(
        evidenceSha256,
        "provider-evidence-json",
        evidenceBytes.byteLength,
        path.relative(this.root, evidenceObject.path),
        capturedAt,
      );
      const existing = db
        .prepare(
          "SELECT revision_id FROM revisions WHERE conversation_key=? AND normalized_sequence_sha256=?",
        )
        .get(key, normalizedSequenceSha256) as { revision_id?: string } | undefined;
      const deduplicated = Boolean(existing?.revision_id);
      const effectiveRevisionId = existing?.revision_id ?? revisionId;
      if (!existing) {
        db.prepare(
          "INSERT INTO revisions(revision_id,conversation_key,normalized_sequence_sha256,raw_sha256,evidence_sha256,current_node_id,turn_count,normalization_version,captured_at) VALUES(?,?,?,?,?,?,?,?,?)",
        ).run(
          effectiveRevisionId,
          key,
          normalizedSequenceSha256,
          rawSha256,
          evidenceSha256,
          parsed.currentNode,
          turns.length,
          TRANSCRIPT_LEDGER_NORMALIZATION,
          capturedAt,
        );
        const turnStmt = db.prepare(
          "INSERT INTO turns(revision_id,ordinal,node_id,parent_id,role,content_type,body_sha256,body_bytes,attachments_json) VALUES(?,?,?,?,?,?,?,?,?)",
        );
        for (const turn of turns)
          turnStmt.run(
            effectiveRevisionId,
            turn.ordinal,
            turn.nodeId,
            turn.parentId,
            turn.role,
            turn.contentType,
            turn.bodySha256,
            turn.bodyBytes,
            stableJson(turn.attachments),
          );
      }
      db.prepare(
        "UPDATE conversations SET latest_revision_id=?,state='captured',updated_at=? WHERE conversation_key=?",
      ).run(effectiveRevisionId, capturedAt, key);
      db.prepare(
        "INSERT INTO observations(observation_id,conversation_key,captured_at,status,raw_sha256,evidence_sha256,normalized_sequence_sha256,revision_id,source_url,capture_method) VALUES(?,?,?,?,?,?,?,?,?,?)",
      ).run(
        observationId,
        key,
        capturedAt,
        "captured",
        rawSha256,
        evidenceSha256,
        normalizedSequenceSha256,
        effectiveRevisionId,
        input.canonicalUrl ?? parsed.url,
        input.captureMethod ?? "provider-native",
      );
      db.exec("COMMIT");
      committed = true;
      return {
        conversationKey: key,
        observationId,
        revisionId: effectiveRevisionId,
        deduplicated,
        rawSha256,
        evidenceSha256,
        normalizedSequenceSha256,
      };
    } catch (error) {
      try {
        db.exec("ROLLBACK");
      } catch {
        /* transaction may already be gone after a crash */
      }
      throw error;
    } finally {
      if (!committed) {
        for (const object of [rawObject, evidenceObject])
          if (object.created) await rm(object.path, { force: true }).catch(() => undefined);
      }
    }
  }

  recordFailedObservation(input: FailedObservationInput): {
    observationId: string;
    conversationKey: string;
  } {
    const conversationId = safeId(input.conversationId, "conversation id");
    const key = conversationKey(input.provider, input.profileId, conversationId);
    const timestamp = input.capturedAt ?? now();
    const db = this.database;
    db.exec("BEGIN IMMEDIATE");
    try {
      db.prepare(`INSERT INTO conversations(conversation_key,provider,profile_id,conversation_id,canonical_url,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(conversation_key) DO UPDATE SET canonical_url=COALESCE(excluded.canonical_url,conversations.canonical_url), state=excluded.state, updated_at=excluded.updated_at`).run(
        key,
        input.provider,
        input.profileId,
        conversationId,
        input.canonicalUrl ?? null,
        input.status,
        timestamp,
        timestamp,
      );
      const observationId = randomUUID();
      db.prepare(
        "INSERT INTO observations(observation_id,conversation_key,captured_at,status,source_url,error_code,error_message) VALUES(?,?,?,?,?,?,?)",
      ).run(
        observationId,
        key,
        timestamp,
        input.status,
        input.canonicalUrl ?? null,
        input.errorCode,
        input.errorMessage?.slice(0, 512) ?? null,
      );
      db.exec("COMMIT");
      return { observationId, conversationKey: key };
    } catch (error) {
      try {
        db.exec("ROLLBACK");
      } catch {
        /* ignore */
      }
      throw error;
    }
  }

  watch(input: {
    provider: string;
    profileId: string;
    conversationId: string;
    canonicalUrl?: string;
    intervalSeconds?: number;
  }): { watchId: string; conversationKey: string } {
    const id = safeId(input.conversationId, "conversation id");
    const key = conversationKey(input.provider, input.profileId, id);
    const watchId = hashText(`${key}\u0000watch`);
    const timestamp = now();
    this.database
      .prepare(
        "INSERT INTO watches(watch_id,provider,profile_id,conversation_id,canonical_url,interval_seconds) VALUES(?,?,?,?,?,?) ON CONFLICT(provider,profile_id,conversation_id) DO UPDATE SET canonical_url=COALESCE(excluded.canonical_url,watches.canonical_url),interval_seconds=excluded.interval_seconds,enabled=1",
      )
      .run(
        watchId,
        input.provider,
        input.profileId,
        id,
        input.canonicalUrl ?? null,
        input.intervalSeconds ?? null,
      );
    this.database
      .prepare(
        "INSERT INTO conversations(conversation_key,provider,profile_id,conversation_id,canonical_url,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(conversation_key) DO UPDATE SET canonical_url=COALESCE(excluded.canonical_url,conversations.canonical_url),updated_at=excluded.updated_at",
      )
      .run(
        key,
        input.provider,
        input.profileId,
        id,
        input.canonicalUrl ?? null,
        "watched",
        timestamp,
        timestamp,
      );
    return { watchId, conversationKey: key };
  }

  list(): LedgerListRow[] {
    return this.database
      .prepare(`SELECT c.conversation_key AS conversationKey,c.provider,c.profile_id AS profileId,c.conversation_id AS conversationId,c.canonical_url AS canonicalUrl,c.state,
      (SELECT COUNT(*) FROM revisions r WHERE r.conversation_key=c.conversation_key) AS revisionCount,
      (SELECT COUNT(*) FROM observations o WHERE o.conversation_key=c.conversation_key) AS observationCount,
      (SELECT MAX(captured_at) FROM revisions r WHERE r.conversation_key=c.conversation_key) AS latestRevisionAt
      FROM conversations c ORDER BY c.updated_at DESC`)
      .all() as unknown as LedgerListRow[];
  }

  watchRows(): Array<Record<string, unknown>> {
    return this.database
      .prepare(
        "SELECT watch_id AS watchId,provider,profile_id AS profileId,conversation_id AS conversationId,canonical_url AS canonicalUrl,enabled,interval_seconds AS intervalSeconds,last_attempt_at AS lastAttemptAt,last_success_at AS lastSuccessAt,last_observation_id AS lastObservationId,next_due_at AS nextDueAt,last_error_code AS lastErrorCode FROM watches ORDER BY watch_id",
      )
      .all() as unknown as Array<Record<string, unknown>>;
  }

  recordWatchAttempt(
    watchId: string,
    result: { observationId?: string; errorCode?: string },
  ): void {
    const timestamp = now();
    this.database
      .prepare(
        "UPDATE watches SET last_attempt_at=?,last_success_at=CASE WHEN ? IS NOT NULL THEN ? ELSE last_success_at END,last_observation_id=COALESCE(?,last_observation_id),last_error_code=? WHERE watch_id=?",
      )
      .run(
        timestamp,
        result.observationId ?? null,
        result.observationId ? timestamp : null,
        result.observationId ?? null,
        result.errorCode ?? null,
        watchId,
      );
  }

  getRevisionTurns(revisionId: string): Array<Record<string, unknown>> {
    return this.database
      .prepare(
        "SELECT ordinal,node_id AS nodeId,parent_id AS parentId,role,content_type AS contentType,body_sha256 AS bodySha256,body_bytes AS bodyBytes,attachments_json AS attachmentsJson FROM turns WHERE revision_id=? ORDER BY ordinal",
      )
      .all(revisionId) as unknown as Array<Record<string, unknown>>;
  }
}

export function canonicalConversationId(value: string): string {
  const id = extractStableConversationIdFromUrl(value) ?? value.trim();
  return safeId(id, "conversation id");
}

/** The only request shape the ledger sync path is allowed to send to Oracle. */
export function buildCaptureOnlySyncRequest(
  conversationUrl: string,
  profileDir?: string,
): {
  prompt: "";
  config: {
    captureOnly: true;
    captureProviderNative: true;
    resumeConversationUrl: string;
    manualLogin: boolean;
    manualLoginProfileDir?: string;
  };
} {
  return {
    prompt: "",
    config: {
      captureOnly: true,
      captureProviderNative: true,
      resumeConversationUrl: conversationUrl,
      manualLogin: Boolean(profileDir),
      ...(profileDir ? { manualLoginProfileDir: profileDir } : {}),
    },
  };
}

export function ledgerWarning(_error: unknown): LedgerWarning {
  return {
    code: "transcript-ledger-ingest-failed",
    severity: "warning",
    message: "Transcript ledger ingest failed; the completed provider capture was retained.",
  };
}

export function deriveChatgptProfileId(config: {
  manualLoginProfileDir?: string | null;
  chromeProfile?: string | null;
  chromeCookiePath?: string | null;
}): string {
  const source =
    config.manualLoginProfileDir ?? config.chromeProfile ?? config.chromeCookiePath ?? "default";
  return `chatgpt-profile-${hashText(path.resolve(source)).slice(0, 32)}`;
}

export async function ingestProviderNativeArtifacts(params: {
  artifacts?: Array<{ path: string; label?: string }>;
  provider?: string;
  profileId: string;
  conversationId?: string;
  canonicalUrl?: string;
  capturedAt?: string;
}): Promise<LedgerIngestResult | undefined> {
  const raw = params.artifacts?.find(
    (artifact) => artifact.label === "provider-native-conversation-raw",
  );
  const evidence = params.artifacts?.find(
    (artifact) => artifact.label === "provider-native-conversation-evidence",
  );
  if (!raw || !evidence) return undefined;
  const ledger = await TranscriptLedger.open();
  try {
    return await ledger.ingestPair({
      provider: params.provider ?? "chatgpt",
      profileId: params.profileId,
      conversationId: params.conversationId,
      canonicalUrl: params.canonicalUrl,
      rawPath: raw.path,
      evidencePath: evidence.path,
      capturedAt: params.capturedAt,
    });
  } finally {
    ledger.close();
  }
}
