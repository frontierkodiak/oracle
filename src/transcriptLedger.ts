import { createHash, randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { chmod, mkdir, open, rename, rm, lstat, readdir } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";
import path from "node:path";
import { getOracleHomeDir } from "./oracleHome.js";
import { extractStableConversationIdFromUrl } from "./browser/conversationUrl.js";
import {
  canonicalNormalizeProviderConversation,
  PROVIDER_NATIVE_MAX_DOCUMENT_BYTES,
} from "./browser/chatgptConversation.js";

export const TRANSCRIPT_LEDGER_SCHEMA = "oracle.transcript-ledger/v1";
export const TRANSCRIPT_LEDGER_NORMALIZATION = "oracle.transcript-ledger-normalized-turns/v1";
export const DEFAULT_LEDGER_DIR_NAME = "transcript-ledger";
const MAX_RAW_BYTES = PROVIDER_NATIVE_MAX_DOCUMENT_BYTES;
const MAX_EVIDENCE_BYTES = 16 * 1024 * 1024;
const MAX_JSON_DEPTH = 128;
const MAX_JSON_NODES = 250_000;
const MAX_GRAPH_NODES = 100_000;
const MAX_TURNS = 25_000;
const MAX_TURN_BODY_BYTES = 16 * 1024 * 1024;

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
  independentPath: string;
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
  independentSha256: string;
  normalizedSequenceSha256: string;
  warning?: LedgerWarning;
}

export interface LedgerWarning {
  code: "transcript-ledger-ingest-failed" | "transcript-ledger-artifact-pair-incomplete";
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
  children?: unknown;
  message?: Record<string, unknown> | null;
};

export class LedgerArtifactPairError extends Error {
  readonly code = "transcript-ledger-artifact-pair-incomplete" as const;

  constructor() {
    super("provider-native raw/evidence/independent artifact set is incomplete");
    this.name = "LedgerArtifactPairError";
  }
}

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

function assertJsonBounds(value: unknown, label: string): void {
  const stack: Array<{ value: unknown; depth: number }> = [{ value, depth: 0 }];
  let count = 0;
  while (stack.length > 0) {
    const entry = stack.pop();
    if (!entry) continue;
    count += 1;
    if (count > MAX_JSON_NODES) throw new Error(`${label} exceeds object-count bounds`);
    if (entry.depth > MAX_JSON_DEPTH) throw new Error(`${label} exceeds depth bounds`);
    if (!entry.value || typeof entry.value !== "object") continue;
    const values = Array.isArray(entry.value)
      ? entry.value
      : Object.values(entry.value as Record<string, unknown>);
    for (const child of values) stack.push({ value: child, depth: entry.depth + 1 });
  }
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
  await assertNoSymlinkAncestors(absolute, absolute);
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
    try {
      await mkdir(dir, { mode: 0o700 });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      const existing = await lstat(dir);
      if (existing.isSymbolicLink() || !existing.isDirectory())
        throw new Error(`ledger path is not a private directory: ${dir}`);
    }
    await chmod(dir, 0o700);
    checkMode((await lstat(dir)).mode, 0o700, dir);
    await syncDirectory(path.dirname(dir));
  }
  // Only ledger-owned directories are normalized; never chmod an ancestor such as /Users.
  if (missing.length === 0 || missing[missing.length - 1] === absolute) {
    await chmod(absolute, 0o700);
    checkMode((await lstat(absolute)).mode, 0o700, absolute);
  }
}

async function assertNoSymlinkAncestors(target: string, protectedRoot?: string): Promise<void> {
  const absolute = path.resolve(target);
  const boundary = protectedRoot ? path.resolve(protectedRoot) : undefined;
  const parsed = path.parse(absolute);
  let current = parsed.root;
  let normalDirectorySeen = false;
  const remainder = absolute.slice(parsed.root.length);
  for (const component of remainder.split(path.sep).filter(Boolean)) {
    current = path.join(current, component);
    try {
      const entry = await lstat(current);
      if (entry.isSymbolicLink()) {
        const towardBoundary = boundary ? path.relative(current, boundary) : "";
        const isBeforeProtectedRoot = Boolean(
          boundary &&
          towardBoundary &&
          !towardBoundary.startsWith("..") &&
          !path.isAbsolute(towardBoundary),
        );
        if (!isBeforeProtectedRoot && (boundary || normalDirectorySeen)) {
          throw new Error(`ledger path may not contain symlinks: ${current}`);
        }
      } else if (entry.isDirectory()) {
        normalDirectorySeen = true;
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
      throw error;
    }
  }
}

function assertContainedPath(root: string, target: string): void {
  const relative = path.relative(path.resolve(root), path.resolve(target));
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error(`ledger path escapes the ledger root: ${target}`);
  }
}

async function syncDirectory(target: string): Promise<void> {
  const handle = await open(target, fsConstants.O_RDONLY);
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

// SQLite remains the interprocess authority. This small in-process queue keeps
// two DatabaseSync connections in one Node event loop from synchronously
// blocking each other while the first publisher is awaiting filesystem I/O.
const processPublicationTails = new Map<string, Promise<void>>();

async function acquireProcessPublicationTurn(root: string): Promise<() => void> {
  const previous = processPublicationTails.get(root) ?? Promise.resolve();
  let releaseTurn!: () => void;
  const turn = new Promise<void>((resolve) => {
    releaseTurn = resolve;
  });
  const tail = previous.then(() => turn);
  processPublicationTails.set(root, tail);
  await previous;
  return () => {
    releaseTurn();
    if (processPublicationTails.get(root) === tail) processPublicationTails.delete(root);
  };
}

async function ensurePrivateFile(target: string): Promise<void> {
  await assertNoSymlinkAncestors(target);
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
    try {
      const handle = await open(index, "wx", 0o600);
      await handle.close();
    } catch (createError) {
      if ((createError as NodeJS.ErrnoException).code !== "EEXIST") throw createError;
      await ensurePrivateFile(index);
    }
    await syncDirectory(root);
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
  assertContainedPath(root, target);
  const parent = path.dirname(target);
  await ensurePrivateDirectory(parent);
  try {
    await ensurePrivateFile(target);
    const existing = await readArtifactBytes(target, bytes.byteLength);
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
    await syncDirectory(parent);
    return { path: target, created: true };
  } catch (error) {
    await handle.close().catch(() => undefined);
    await rm(temp, { force: true }).catch(() => undefined);
    throw error;
  }
}

async function readArtifactBytes(target: string, maxBytes: number): Promise<Buffer> {
  await assertNoSymlinkAncestors(target);
  const handle = await open(target, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
  try {
    const entry = await handle.stat();
    if (!entry.isFile()) throw new Error(`capture artifact must be a regular file: ${target}`);
    if (!Number.isSafeInteger(entry.size) || entry.size < 0 || entry.size > maxBytes) {
      throw new Error(`capture artifact exceeds byte bounds: ${target}`);
    }
    const bytes = Buffer.allocUnsafe(entry.size);
    let offset = 0;
    while (offset < bytes.byteLength) {
      const result = await handle.read(bytes, offset, bytes.byteLength - offset, offset);
      if (result.bytesRead === 0)
        throw new Error(`capture artifact changed while reading: ${target}`);
      offset += result.bytesRead;
    }
    const after = await handle.stat();
    if (after.size !== entry.size)
      throw new Error(`capture artifact changed while reading: ${target}`);
    return bytes;
  } finally {
    await handle.close();
  }
}

async function recoverObjectStore(root: string, db: DatabaseSync): Promise<void> {
  const objectRoot = path.join(root, "objects", "sha256");
  const prefixes = await readdir(objectRoot, { withFileTypes: true });
  for (const prefix of prefixes) {
    const prefixPath = path.join(objectRoot, prefix.name);
    if (prefix.isSymbolicLink() || !prefix.isDirectory()) {
      throw new Error(`ledger object store contains an unsafe descendant: ${prefixPath}`);
    }
    await ensurePrivateDirectory(prefixPath);
    const entries = await readdir(prefixPath, { withFileTypes: true });
    for (const entry of entries) {
      const target = path.join(prefixPath, entry.name);
      if (entry.isSymbolicLink())
        throw new Error(`ledger object store contains a symlink: ${target}`);
      if (!entry.isFile()) throw new Error(`ledger object store contains a non-file: ${target}`);
      if (entry.name.startsWith(".")) {
        await rm(target, { force: true });
        await syncDirectory(prefixPath);
        continue;
      }
      if (!/^[a-f0-9]{64}$/.test(entry.name) || entry.name.slice(0, 2) !== prefix.name) {
        throw new Error(`ledger object has an invalid name: ${target}`);
      }
      await ensurePrivateFile(target);
      const bytes = await readArtifactBytes(target, MAX_RAW_BYTES);
      if (hashBytes(bytes) !== entry.name)
        throw new Error(`ledger object hash mismatch: ${target}`);
      const row = db.prepare("SELECT path FROM objects WHERE sha256=?").get(entry.name) as
        | { path?: string }
        | undefined;
      if (!row) {
        await rm(target, { force: true });
        await syncDirectory(prefixPath);
      } else if (row.path !== path.relative(root, target)) {
        throw new Error(`ledger object index path mismatch: ${target}`);
      }
    }
  }
  const rows = db.prepare("SELECT sha256,path FROM objects").all() as Array<{
    sha256: string;
    path: string;
  }>;
  for (const row of rows) {
    const target = path.resolve(root, row.path);
    assertContainedPath(root, target);
    try {
      await ensurePrivateFile(target);
      const bytes = await readArtifactBytes(target, MAX_RAW_BYTES);
      if (hashBytes(bytes) !== row.sha256)
        throw new Error(`ledger object hash mismatch: ${target}`);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        throw new Error(`ledger index references missing object: ${row.sha256}`);
      }
      throw error;
    }
  }
}

function parseConversation(
  raw: Record<string, unknown>,
  supplied?: string,
): {
  id: string;
  url: string | null;
  currentNode: string | null;
  mapping: Record<string, RawNode>;
  selectedChain: string[];
} {
  if (supplied && typeof raw.conversation_id === "string" && raw.conversation_id !== supplied) {
    throw new Error("raw conversation id does not match supplied conversation id");
  }
  const id = safeId(supplied ?? String(raw.conversation_id ?? raw.id ?? ""), "conversation id");
  const url = typeof raw.conversation_url === "string" ? raw.conversation_url : null;
  if (!raw.mapping || typeof raw.mapping !== "object" || Array.isArray(raw.mapping)) {
    throw new Error("provider raw document has no object mapping");
  }
  const mapping = raw.mapping as Record<string, RawNode>;
  const nodeIds = Object.keys(mapping);
  if (nodeIds.length === 0 || nodeIds.length > MAX_GRAPH_NODES) {
    throw new Error("provider raw mapping has an invalid node count");
  }
  const current = raw.current_node;
  if (typeof current !== "string" || !Object.hasOwn(mapping, current)) {
    throw new Error("provider raw current_node is missing from mapping");
  }
  const roots: string[] = [];
  for (const nodeId of nodeIds) {
    const node = mapping[nodeId];
    if (!node || typeof node !== "object" || Array.isArray(node)) {
      throw new Error(`provider raw mapping node is not an object: ${nodeId}`);
    }
    if (node.id !== nodeId) throw new Error(`provider raw mapping node id mismatch: ${nodeId}`);
    if (!("parent" in node)) throw new Error(`provider raw mapping node has no parent: ${nodeId}`);
    if (
      node.message !== null &&
      (typeof node.message !== "object" || Array.isArray(node.message))
    ) {
      throw new Error(`provider raw mapping message is invalid: ${nodeId}`);
    }
    if (node.message) {
      const author = node.message.author;
      const content = node.message.content;
      if (
        !author ||
        typeof author !== "object" ||
        Array.isArray(author) ||
        typeof (author as Record<string, unknown>).role !== "string"
      ) {
        throw new Error(`provider raw mapping message author is invalid: ${nodeId}`);
      }
      if (!content || typeof content !== "object" || Array.isArray(content)) {
        throw new Error(`provider raw mapping message content is invalid: ${nodeId}`);
      }
    }
    if (node.parent === null) roots.push(nodeId);
    else if (typeof node.parent !== "string" || !Object.hasOwn(mapping, node.parent)) {
      throw new Error(`provider raw mapping parent is invalid: ${nodeId}`);
    }
    if (!("children" in node) || !Array.isArray(node.children)) {
      throw new Error(`provider raw mapping node has no children array: ${nodeId}`);
    }
    const children = node.children as unknown[];
    if (
      new Set(children).size !== children.length ||
      children.some((child) => typeof child !== "string")
    ) {
      throw new Error(`provider raw mapping children are invalid: ${nodeId}`);
    }
    for (const childValue of children) {
      const child = childValue as string;
      if (!Object.hasOwn(mapping, child))
        throw new Error(`provider raw child is missing: ${child}`);
      if (mapping[child]?.parent !== nodeId)
        throw new Error(`provider raw parent/child mismatch: ${nodeId}`);
    }
  }
  if (roots.length !== 1) throw new Error("provider raw mapping must have exactly one root");
  const visited = new Set<string>();
  const stack = [...roots];
  while (stack.length > 0) {
    const nodeId = stack.pop();
    if (!nodeId || visited.has(nodeId)) continue;
    visited.add(nodeId);
    const children = mapping[nodeId]?.children;
    if (!Array.isArray(children)) throw new Error(`provider raw children are missing: ${nodeId}`);
    for (const child of children as string[]) stack.push(child);
  }
  if (visited.size !== nodeIds.length)
    throw new Error("provider raw mapping contains a disconnected or cyclic graph");
  const selectedChain: string[] = [];
  const selectedSeen = new Set<string>();
  let cursor: string | null = current;
  while (cursor !== null) {
    if (selectedSeen.has(cursor)) throw new Error("provider raw selected branch contains a cycle");
    selectedSeen.add(cursor);
    selectedChain.push(cursor);
    const parent: unknown = mapping[cursor]?.parent;
    cursor = typeof parent === "string" ? parent : null;
  }
  if (
    selectedChain[selectedChain.length - 1] === undefined ||
    mapping[selectedChain[selectedChain.length - 1] as string]?.parent !== null
  ) {
    throw new Error("provider raw selected branch does not terminate at a root");
  }
  selectedChain.reverse();
  return { id, url, currentNode: current, mapping, selectedChain };
}

function normalizedAttachments(value: unknown): unknown[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((entry): entry is Record<string, unknown> =>
      Boolean(entry && typeof entry === "object" && !Array.isArray(entry)),
    )
    .map((entry) => ({
      name: typeof entry.name === "string" ? entry.name : null,
      bytes:
        typeof entry.size === "number"
          ? entry.size
          : typeof entry.bytes === "number"
            ? entry.bytes
            : null,
      mimeType:
        typeof entry.mime_type === "string"
          ? entry.mime_type
          : typeof entry.mimeType === "string"
            ? entry.mimeType
            : null,
    }));
}

function decimalDigest(value: unknown, label: string): Buffer {
  if (typeof value !== "string")
    throw new Error(`${label} must be a space-separated decimal byte string`);
  const pieces = value.trim().split(/\s+/);
  if (pieces.length !== 32) throw new Error(`${label} must contain exactly 32 decimal bytes`);
  const bytes = pieces.map((piece) => {
    if (!/^\d+$/.test(piece)) throw new Error(`${label} contains a non-decimal byte`);
    const parsed = Number(piece);
    if (!Number.isInteger(parsed) || parsed < 0 || parsed > 255)
      throw new Error(`${label} contains an invalid byte`);
    return parsed;
  });
  return Buffer.from(bytes);
}

function selectedTurns(
  parsed: ReturnType<typeof parseConversation>,
  rawText: string,
): NormalizedTurn[] {
  const canonical = canonicalNormalizeProviderConversation(rawText);
  const messageNodes = parsed.selectedChain.filter((nodeId) => {
    const node = parsed.mapping[nodeId];
    const message = node?.message;
    if (!message || typeof message !== "object") return false;
    const role =
      typeof message.author === "object" && message.author !== null
        ? String((message.author as Record<string, unknown>).role ?? "unknown")
        : "unknown";
    return role !== "system";
  });
  if (canonical.length !== messageNodes.length) {
    throw new Error("canonical provider normalizer disagrees with selected branch");
  }
  const turns = canonical.map((normalized, ordinal) => {
    const nodeId = messageNodes[ordinal];
    const node = parsed.mapping[nodeId];
    if (!node) throw new Error("canonical provider normalizer selected an unknown node");
    const message = node.message;
    const role =
      message && typeof message.author === "object" && message.author !== null
        ? String((message.author as Record<string, unknown>).role ?? "unknown")
        : "unknown";
    if (normalized.role !== role)
      throw new Error(`canonical provider role mismatch at turn ${ordinal}`);
    const bodyBytes = Buffer.byteLength(normalized.body, "utf8");
    if (bodyBytes > MAX_TURN_BODY_BYTES) throw new Error("provider turn body exceeds bounds");
    const attachments = normalizedAttachments(normalized.attachments);
    return {
      ordinal,
      nodeId,
      parentId: typeof node.parent === "string" ? node.parent : null,
      role,
      contentType: normalized.contentType,
      body: normalized.body,
      bodySha256: hashText(normalized.body),
      bodyBytes,
      attachments,
    };
  });
  if (turns.length > MAX_TURNS)
    throw new Error("provider selected branch exceeds turn-count bounds");
  return turns;
}

function validateEvidence(
  evidence: Record<string, unknown>,
  parsed: ReturnType<typeof parseConversation>,
  turns: NormalizedTurn[],
  independentParsed: ReturnType<typeof parseConversation>,
  independentTurns: NormalizedTurn[],
  rawSha256: string,
  rawBytes: number,
  independentSha256: string,
  independentBytes: number,
): void {
  if (evidence.schema !== "oracle.provider-native-capture-evidence/v1") {
    throw new Error("unsupported provider evidence schema");
  }
  if (evidence.conversation_id !== parsed.id) {
    throw new Error("raw and evidence conversation ids do not match");
  }
  const materialized = evidence.materialized_document;
  if (!materialized || typeof materialized !== "object" || Array.isArray(materialized)) {
    throw new Error("evidence has no materialized document descriptor");
  }
  const materializedRecord = materialized as Record<string, unknown>;
  if (
    Object.keys(materializedRecord).some((key) => !["sha256", "bytes"].includes(key)) ||
    !Object.hasOwn(materializedRecord, "sha256") ||
    !Object.hasOwn(materializedRecord, "bytes")
  ) {
    throw new Error("evidence materialized document descriptor has an unsupported shape");
  }
  if (
    typeof materializedRecord.sha256 !== "string" ||
    !/^[a-f0-9]{64}$/.test(materializedRecord.sha256)
  ) {
    throw new Error("evidence materialized document hash is invalid");
  }
  if (
    typeof materializedRecord.bytes !== "number" ||
    !Number.isSafeInteger(materializedRecord.bytes)
  ) {
    throw new Error("evidence materialized document byte count is invalid");
  }
  if (materializedRecord.sha256 !== rawSha256 || materializedRecord.bytes !== rawBytes) {
    throw new Error("evidence does not describe the supplied raw bytes");
  }
  if (independentParsed.id !== parsed.id) {
    throw new Error("independent document conversation id does not match raw document");
  }
  const independentDocument = evidence.independent_document;
  if (
    !independentDocument ||
    typeof independentDocument !== "object" ||
    Array.isArray(independentDocument)
  ) {
    throw new Error("evidence has no independent document descriptor");
  }
  const independentDocumentRecord = independentDocument as Record<string, unknown>;
  if (
    Object.keys(independentDocumentRecord).some((key) => !["sha256", "bytes"].includes(key)) ||
    !Object.hasOwn(independentDocumentRecord, "sha256") ||
    !Object.hasOwn(independentDocumentRecord, "bytes") ||
    typeof independentDocumentRecord.sha256 !== "string" ||
    !/^[a-f0-9]{64}$/.test(independentDocumentRecord.sha256) ||
    typeof independentDocumentRecord.bytes !== "number" ||
    !Number.isSafeInteger(independentDocumentRecord.bytes) ||
    independentDocumentRecord.bytes <= 0 ||
    independentDocumentRecord.bytes > MAX_RAW_BYTES
  ) {
    throw new Error("evidence independent document descriptor has an unsupported shape");
  }
  if (
    independentDocumentRecord.sha256 !== independentSha256 ||
    independentDocumentRecord.bytes !== independentBytes
  ) {
    throw new Error("evidence does not describe the supplied independent document");
  }
  const independent = evidence.independent_fetch;
  if (!independent || typeof independent !== "object" || Array.isArray(independent)) {
    throw new Error("evidence has no independent fetch descriptor");
  }
  const independentRecord = independent as Record<string, unknown>;
  const independentKeys = Object.keys(independentRecord).sort();
  if (
    independentKeys.join("\u0000") !==
    ["document_bytes", "document_sha256_decimal_bytes", "fetched_at"].join("\u0000")
  ) {
    throw new Error("evidence independent fetch descriptor has an unsupported shape");
  }
  const independentDigest = decimalDigest(
    independentRecord.document_sha256_decimal_bytes,
    "independent fetch document digest",
  );
  if (
    typeof independentRecord.document_bytes !== "number" ||
    !Number.isSafeInteger(independentRecord.document_bytes) ||
    independentRecord.document_bytes <= 0 ||
    independentRecord.document_bytes > MAX_RAW_BYTES
  ) {
    throw new Error("independent fetch document byte count is invalid");
  }
  if (
    independentRecord.document_bytes !== independentBytes ||
    Buffer.from(independentDigest).toString("hex") !== independentSha256
  ) {
    throw new Error("independent fetch descriptor does not describe the supplied document");
  }
  if (
    typeof independentRecord.fetched_at !== "string" ||
    !Number.isFinite(Date.parse(independentRecord.fetched_at))
  ) {
    throw new Error("independent fetch timestamp is invalid");
  }
  const perTurn = evidence.per_turn;
  if (!Array.isArray(perTurn)) throw new Error("evidence has no per-turn digest array");
  const expected = independentTurns.filter((turn) => turn.role !== "system");
  if (perTurn.length !== expected.length)
    throw new Error("evidence turn count does not match selected branch");
  for (let index = 0; index < expected.length; index += 1) {
    const actual = perTurn[index];
    const turn = expected[index];
    if (!actual || typeof actual !== "object" || Array.isArray(actual))
      throw new Error(`evidence turn ${index} is not an object`);
    const record = actual as Record<string, unknown>;
    if (
      record.i !== index ||
      record.role !== turn.role ||
      record.ct !== turn.contentType ||
      record.blen !== turn.bodyBytes
    ) {
      throw new Error(`evidence turn ${index} does not correspond to the selected branch`);
    }
    if (typeof record.sha256_hex !== "string" || !/^[a-f0-9]{64}$/.test(record.sha256_hex))
      throw new Error(`evidence turn ${index} hex hash is invalid`);
    if (record.sha256_hex !== turn.bodySha256)
      throw new Error(`evidence turn ${index} body hash mismatch`);
    const decimal = record.sha256_dec;
    const expectedDecimal = [...Buffer.from(turn.bodySha256, "hex")];
    const decimalValues = [...decimalDigest(decimal, `evidence turn ${index} decimal hash`)];
    if (
      decimalValues.length !== expectedDecimal.length ||
      decimalValues.some((value, i) => value !== expectedDecimal[i])
    ) {
      throw new Error(`evidence turn ${index} decimal hash mismatch`);
    }
    if (stableJson(normalizedAttachments(record.attachments)) !== stableJson(turn.attachments)) {
      throw new Error(`evidence turn ${index} attachment metadata mismatch`);
    }
  }
  const authoritative = turns.filter((turn) => turn.role !== "system");
  if (
    authoritative.length !== expected.length ||
    authoritative.some(
      (turn, index) =>
        turn.role !== expected[index].role ||
        turn.contentType !== expected[index].contentType ||
        turn.bodySha256 !== expected[index].bodySha256 ||
        turn.bodyBytes !== expected[index].bodyBytes ||
        stableJson(turn.attachments) !== stableJson(expected[index].attachments),
    )
  ) {
    throw new Error("independent document selected branch differs from authoritative raw");
  }
}

function conversationKey(provider: string, profileId: string, conversationId: string): string {
  return hashText(
    `${safeId(provider, "provider")}\u0000${safeId(profileId, "profile id")}\u0000${safeId(conversationId, "conversation id")}`,
  );
}

function createDb(indexPath: string): DatabaseSync {
  const db = new DatabaseSync(indexPath);
  db.exec(
    "PRAGMA busy_timeout=30000; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
  );
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
      normalized_sequence_sha256 TEXT NOT NULL, raw_sha256 TEXT NOT NULL, evidence_sha256 TEXT NOT NULL, independent_sha256 TEXT NOT NULL,
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
      captured_at TEXT NOT NULL, status TEXT NOT NULL, raw_sha256 TEXT, evidence_sha256 TEXT, independent_sha256 TEXT,
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
  const ensureColumn = (table: string, column: string, definition: string) => {
    const columns = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name?: string }>;
    if (!columns.some((entry) => entry.name === column)) {
      db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
    }
  };
  ensureColumn("revisions", "independent_sha256", "TEXT");
  ensureColumn("observations", "independent_sha256", "TEXT");
  db.prepare("INSERT OR IGNORE INTO ledger_meta(key,value) VALUES('schema', ?)").run(
    TRANSCRIPT_LEDGER_SCHEMA,
  );
  return db;
}

export class TranscriptLedger {
  readonly root: string;
  private db?: DatabaseSync;
  private activeOperations = 0;
  private closeRequested = false;
  private waitingOperations = 0;
  private queuedCloseRequested = false;
  private activePublicationOperations = 0;

  private constructor(root: string) {
    this.root = root;
  }

  private beginOperation(): void {
    if (this.closeRequested || !this.db) throw new Error("transcript ledger is closed");
    this.activeOperations += 1;
  }

  private endOperation(): void {
    this.activeOperations -= 1;
    if (this.activeOperations === 0 && this.closeRequested) {
      this.db?.close();
      this.db = undefined;
    }
  }

  private async acquirePublicationTurnForOperation(): Promise<() => void> {
    this.waitingOperations += 1;
    try {
      const release = await acquireProcessPublicationTurn(this.root);
      if (this.queuedCloseRequested) {
        release();
        throw new Error("transcript ledger is closed");
      }
      return release;
    } finally {
      this.waitingOperations -= 1;
    }
  }

  static async open(options: TranscriptLedgerOptions = {}): Promise<TranscriptLedger> {
    const root = resolveTranscriptLedgerRoot(options.root);
    await ensureRoot(root);
    const ledger = new TranscriptLedger(root);
    const releaseTurn = await acquireProcessPublicationTurn(root);
    try {
      ledger.db = createDb(path.join(root, "index.sqlite"));
      await ensurePrivateFile(path.join(root, "index.sqlite-wal")).catch((error) => {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      });
      ledger.database.exec("BEGIN IMMEDIATE");
      try {
        await recoverObjectStore(root, ledger.database);
        ledger.database.exec("COMMIT");
      } catch (error) {
        try {
          ledger.database.exec("ROLLBACK");
        } catch {
          /* recovery transaction may already be gone after an interrupted open */
        }
        throw error;
      }
    } catch (error) {
      ledger.close();
      throw error;
    } finally {
      releaseTurn();
    }
    return ledger;
  }

  close(): void {
    this.closeRequested = true;
    if (this.waitingOperations > 0) this.queuedCloseRequested = true;
    if (this.activeOperations === 0) {
      this.db?.close();
      this.db = undefined;
    }
  }

  private get database(): DatabaseSync {
    if (!this.db) throw new Error("transcript ledger is closed");
    return this.db;
  }

  async ingestPair(input: IngestPairInput): Promise<LedgerIngestResult> {
    this.beginOperation();
    let releaseTurn: (() => void) | undefined;
    try {
      return await this.ingestPairActive(input, (release) => {
        releaseTurn = release;
      });
    } finally {
      releaseTurn?.();
      this.endOperation();
    }
  }

  private async ingestPairActive(
    input: IngestPairInput,
    setReleaseTurn: (release: () => void) => void,
  ): Promise<LedgerIngestResult> {
    const rawBytes = await readArtifactBytes(input.rawPath, MAX_RAW_BYTES);
    const evidenceBytes = await readArtifactBytes(input.evidencePath, MAX_EVIDENCE_BYTES);
    const independentBytes = await readArtifactBytes(input.independentPath, MAX_RAW_BYTES);
    let raw: Record<string, unknown>;
    let evidence: Record<string, unknown>;
    let independent: Record<string, unknown>;
    try {
      raw = JSON.parse(rawBytes.toString("utf8")) as Record<string, unknown>;
      assertJsonBounds(raw, "provider raw document");
    } catch (error) {
      throw new Error(
        `provider raw document is invalid: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    const parsed = parseConversation(raw, input.conversationId);
    try {
      evidence = JSON.parse(evidenceBytes.toString("utf8")) as Record<string, unknown>;
      assertJsonBounds(evidence, "provider evidence document");
    } catch (error) {
      throw new Error(
        `provider evidence document is invalid: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    try {
      independent = JSON.parse(independentBytes.toString("utf8")) as Record<string, unknown>;
      assertJsonBounds(independent, "independent provider document");
    } catch (error) {
      throw new Error(
        `independent provider document is invalid: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    const rawSha256 = hashBytes(rawBytes);
    const evidenceSha256 = hashBytes(evidenceBytes);
    const independentSha256 = hashBytes(independentBytes);
    const turns = selectedTurns(parsed, rawBytes.toString("utf8"));
    const independentParsed = parseConversation(independent, input.conversationId);
    const independentTurns = selectedTurns(independentParsed, independentBytes.toString("utf8"));
    validateEvidence(
      evidence,
      parsed,
      turns,
      independentParsed,
      independentTurns,
      rawSha256,
      rawBytes.byteLength,
      independentSha256,
      independentBytes.byteLength,
    );
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
    const acquiredReleaseTurn = await this.acquirePublicationTurnForOperation();
    this.activePublicationOperations += 1;
    setReleaseTurn(() => {
      this.activePublicationOperations -= 1;
      acquiredReleaseTurn();
    });
    let rawObject: { path: string; created: boolean } | undefined;
    let evidenceObject: { path: string; created: boolean } | undefined;
    let independentObject: { path: string; created: boolean } | undefined;
    const db = this.database;
    let transactionStarted = false;
    try {
      db.exec("BEGIN IMMEDIATE");
      transactionStarted = true;
      rawObject = await writeObject(this.root, rawBytes, rawSha256);
      evidenceObject = await writeObject(this.root, evidenceBytes, evidenceSha256);
      independentObject = await writeObject(this.root, independentBytes, independentSha256);
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
        independentSha256,
        "independent-provider-json",
        independentBytes.byteLength,
        path.relative(this.root, independentObject.path),
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
          "INSERT INTO revisions(revision_id,conversation_key,normalized_sequence_sha256,raw_sha256,evidence_sha256,independent_sha256,current_node_id,turn_count,normalization_version,captured_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
        ).run(
          effectiveRevisionId,
          key,
          normalizedSequenceSha256,
          rawSha256,
          evidenceSha256,
          independentSha256,
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
        "INSERT INTO observations(observation_id,conversation_key,captured_at,status,raw_sha256,evidence_sha256,independent_sha256,normalized_sequence_sha256,revision_id,source_url,capture_method) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
      ).run(
        observationId,
        key,
        capturedAt,
        "captured",
        rawSha256,
        evidenceSha256,
        independentSha256,
        normalizedSequenceSha256,
        effectiveRevisionId,
        input.canonicalUrl ?? parsed.url,
        input.captureMethod ?? "provider-native",
      );
      db.exec("COMMIT");
      return {
        conversationKey: key,
        observationId,
        revisionId: effectiveRevisionId,
        deduplicated,
        rawSha256,
        evidenceSha256,
        independentSha256,
        normalizedSequenceSha256,
      };
    } catch (error) {
      if (transactionStarted) {
        try {
          db.exec("ROLLBACK");
        } catch {
          /* transaction may already be gone after a crash */
        }
      }
      throw error;
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
    if (
      input.intervalSeconds !== undefined &&
      (!Number.isFinite(input.intervalSeconds) || input.intervalSeconds <= 0)
    ) {
      throw new Error("watch interval must be a positive finite number");
    }
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
        "UPDATE watches SET last_attempt_at=?,last_success_at=CASE WHEN ? IS NULL THEN ? ELSE last_success_at END,last_observation_id=COALESCE(?,last_observation_id),last_error_code=? WHERE watch_id=?",
      )
      .run(
        timestamp,
        result.errorCode ?? null,
        timestamp,
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

export function selectSyncWatches(
  rows: Array<Record<string, unknown>>,
  conversationId: string | undefined,
  all: boolean,
): Array<Record<string, unknown>> {
  if (!conversationId && !all) throw new Error("sync requires a conversation argument or --all");
  return rows.filter(
    (row) => Number(row.enabled) === 1 && (all || row.conversationId === conversationId),
  );
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
  if (_error instanceof LedgerArtifactPairError) {
    return {
      code: "transcript-ledger-artifact-pair-incomplete",
      severity: "warning",
      message:
        "Provider-native capture returned an incomplete raw/evidence/independent artifact set; the completed provider result was retained.",
    };
  }
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
  const stableSource = path.isAbsolute(source) ? source : path.resolve(getOracleHomeDir(), source);
  return `chatgpt-profile-${hashText(stableSource).slice(0, 32)}`;
}

export function parsePositiveFiniteInterval(value: string): number {
  const interval = Number(value);
  if (!Number.isFinite(interval) || interval <= 0) {
    throw new Error("interval must be a positive finite number");
  }
  return interval;
}

export function classifyObservationFailure(
  error: unknown,
): Exclude<LedgerObservationStatus, "captured"> {
  const candidate = error as { message?: unknown; details?: unknown };
  let detailText = "";
  try {
    detailText = JSON.stringify(candidate?.details ?? "");
  } catch {
    detailText = String(candidate?.details ?? "");
  }
  const text = `${String(candidate?.message ?? "")} ${detailText}`.toLowerCase();
  if (text.includes("challenged") || text.includes("bot mitigation")) return "challenged";
  if (
    text.includes("auth-session-unavailable") ||
    text.includes("authentication unavailable") ||
    text.includes("not authenticated")
  ) {
    return "auth-unavailable";
  }
  return "failed";
}

export async function ingestProviderNativeArtifacts(params: {
  artifacts?: Array<{ path: string; label?: string }>;
  requirePair?: boolean;
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
  const independent = params.artifacts?.find(
    (artifact) => artifact.label === "provider-native-conversation-independent",
  );
  if (!raw && !evidence && !independent) {
    if (params.requirePair) throw new LedgerArtifactPairError();
    return undefined;
  }
  if (!raw || !evidence || !independent) throw new LedgerArtifactPairError();
  const ledger = await TranscriptLedger.open();
  try {
    return await ledger.ingestPair({
      provider: params.provider ?? "chatgpt",
      profileId: params.profileId,
      conversationId: params.conversationId,
      canonicalUrl: params.canonicalUrl,
      rawPath: raw.path,
      evidencePath: evidence.path,
      independentPath: independent.path,
      capturedAt: params.capturedAt,
    });
  } finally {
    ledger.close();
  }
}
