import http from "node:http";
import path from "node:path";
import { createHash, randomBytes } from "node:crypto";
import { mkdir, open, readFile, rename, rm, stat, chmod, lstat } from "node:fs/promises";
import { createWriteStream } from "node:fs";
import { constants as fsConstants } from "node:fs";
import { Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { BrowserRunOptions, BrowserRunResult } from "../browserMode.js";
import type { BrowserAttachment, SavedBrowserFile } from "../browser/types.js";
import {
  computeFileSha256,
  resolveSessionArtifactsDir,
  resolveUniqueArtifactPath,
  sanitizeArtifactFilename,
  sanitizeArtifactMimeType,
  validateArtifactFile,
} from "../browser/artifacts.js";
import { getOracleHomeDir } from "../oracleHome.js";
import { parseHostPort } from "../bridge/connection.js";
import { checkRemoteHealth } from "./health.js";
import {
  ARTIFACT_TRANSFER_FEATURE_ID,
  CAPTURE_ONLY_FEATURE_ID,
  DURABLE_QUEUE_FEATURE_ID,
  MAX_REMOTE_ARTIFACT_BYTES,
  type DurableRunSnapshot,
  type RemoteArtifactDescriptor,
  type RemoteAttachmentPayload,
  type RemoteCapabilityRequirement,
  type RemoteRunPayload,
} from "./types.js";

const DEFAULT_TIMEOUT_MS = 15_000;
const TERMINAL = new Set(["completed", "failed", "canceled", "unknown"]);
export interface RemoteExecutorOptions {
  host: string;
  token?: string;
  requiredCapabilities?: RemoteCapabilityRequirement[];
}
export interface DurableReceipt {
  sessionId: string;
  idempotencyKey: string;
  runId?: string;
  payloadHash?: string;
}
export interface DurableWatchOptions {
  token?: string;
  signal?: AbortSignal;
  timeoutMs?: number;
  reconnectDelayMs?: number;
  pollMs?: number;
  onSnapshot?: (snapshot: DurableRunSnapshot) => void;
}
export type DurableWatchOutcome = { snapshot: DurableRunSnapshot; detached: boolean };
export interface QueueStatus {
  active: number;
  queued: number;
  capacity: number;
  [key: string]: unknown;
}

export function receiptPath(sessionId: string): string {
  if (!/^[a-zA-Z0-9_-]{1,128}$/.test(sessionId)) throw new Error("invalid Oracle session id");
  return path.join(getOracleHomeDir(), "sessions", sessionId, "durable-queue.json");
}
async function privateReceiptParent(target: string): Promise<void> {
  const root = getOracleHomeDir();
  const dirs = [root, path.join(root, "sessions"), path.dirname(target)];
  for (const dir of dirs) {
    await mkdir(dir, { recursive: true, mode: 0o700 });
    const info = await lstat(dir);
    if (!info.isDirectory() || info.isSymbolicLink())
      throw new Error("unsafe durable receipt directory");
    await chmod(dir, 0o700);
  }
}
export async function readDurableReceipt(sessionId: string): Promise<DurableReceipt | undefined> {
  try {
    const target = receiptPath(sessionId);
    const handle = await open(target, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
    try {
      const info = await handle.stat();
      if (!info.isFile()) throw new Error("unsafe durable receipt file");
      await chmod(target, 0o600);
      return validateReceipt(JSON.parse(await handle.readFile("utf8")), sessionId);
    } finally { await handle.close(); }
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw new Error("invalid durable queue receipt");
  }
}
export async function writeDurableReceipt(receipt: DurableReceipt): Promise<void> {
  validateReceipt(receipt, receipt.sessionId);
  const target = receiptPath(receipt.sessionId);
  await privateReceiptParent(target);
  const temp = `${target}.tmp-${randomBytes(8).toString("hex")}`;
  const handle = await open(temp, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(receipt)}\n`);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(temp, target);
  await chmod(target, 0o600);
  const dir = await open(path.dirname(target), "r");
  try {
    await dir.sync();
  } finally {
    await dir.close();
  }
  await rm(temp, { force: true });
}
function validateReceipt(value: unknown, sessionId: string): DurableReceipt {
  const r = value as Record<string, unknown>;
  if (
    !r ||
    r.sessionId !== sessionId ||
    typeof r.idempotencyKey !== "string" ||
    !/^[a-f0-9]{64}$/.test(r.idempotencyKey) ||
    (r.runId !== undefined &&
      (typeof r.runId !== "string" || !/^[a-zA-Z0-9_-]{1,128}$/.test(r.runId))) ||
    (r.payloadHash !== undefined &&
      (typeof r.payloadHash !== "string" || !/^[a-f0-9]{64}$/.test(r.payloadHash)))
  )
    throw new Error("invalid durable queue receipt");
  return r as unknown as DurableReceipt;
}

export async function submitDurableRemoteRun(p: {
  host: string;
  token?: string;
  idempotencyKey: string;
  payload: RemoteRunPayload;
}): Promise<DurableRunSnapshot> {
  if (!/^[a-f0-9]{64}$/.test(p.idempotencyKey))
    throw new Error("idempotency key must be 32 cryptorandom bytes");
  const s = await requestDurableJson({ ...p, method: "POST", path: "/v1/runs" });
  validateSnapshot(s);
  return s;
}
export async function getDurableRemoteRun(
  host: string,
  id: string,
  token?: string,
): Promise<DurableRunSnapshot> {
  const s = await requestDurableJson({
    host,
    token,
    method: "GET",
    path: `/v1/runs/${encodeURIComponent(id)}`,
  });
  validateSnapshot(s);
  return s;
}
export async function getDurableRemoteRunEvents(
  host: string,
  id: string,
  after = -1,
  token?: string,
): Promise<Array<{ seq: number; event: unknown }>> {
  const v = await requestDurableJson({
    host,
    token,
    method: "GET",
    path: `/v1/runs/${encodeURIComponent(id)}/events?after=${encodeURIComponent(String(after))}`,
  });
  if (
    !v ||
    !Array.isArray(v.events) ||
    v.events.some(
      (e: unknown) => !e || typeof e !== "object" || !Number.isSafeInteger((e as any).seq),
    )
  )
    throw new Error("malformed durable events response");
  let prior = after;
  for (const e of v.events) {
    if ((e as any).seq <= prior) throw new Error("nonmonotonic durable events response");
    prior = (e as any).seq;
  }
  return v.events;
}
export async function getDurableRemoteQueueStatus(
  host: string,
  token?: string,
): Promise<QueueStatus> {
  const value = await requestDurableJson({ host, token, method: "GET", path: "/v1/queue/status" });
  if (
    !value ||
    !Number.isSafeInteger(value.active) ||
    !Number.isSafeInteger(value.queued) ||
    !Number.isSafeInteger(value.capacity) ||
    value.active < 0 ||
    value.queued < 0 ||
    value.capacity < 1
  )
    throw new Error("malformed durable queue status");
  return value;
}
export async function cancelDurableRemoteRun(
  host: string,
  id: string,
  token?: string,
): Promise<DurableRunSnapshot> {
  const s = await requestDurableJson({
    host,
    token,
    method: "POST",
    path: `/v1/runs/${encodeURIComponent(id)}/cancel`,
  });
  validateSnapshot(s);
  return s;
}

export async function watchDurableRemoteRun(
  host: string,
  id: string,
  o: DurableWatchOptions = {},
): Promise<DurableWatchOutcome> {
  let after = -1;
  let detached = false;
  const deadline = Date.now() + (o.timeoutMs ?? 600_000);
  for (;;) {
    if (o.signal?.aborted) throw new Error("observer aborted");
    try {
      const events = await getDurableRemoteRunEvents(host, id, after, o.token);
      for (const e of events) after = Math.max(after, e.seq);
      const s = await getDurableRemoteRun(host, id, o.token);
      o.onSnapshot?.(s);
      if (TERMINAL.has(s.state)) return { snapshot: s, detached };
    } catch (e) {
      if (!isRetryableTransport(e)) throw e;
      detached = true;
      if (Date.now() >= deadline)
        throw new Error(`durable run observer timed out while detached: ${safeMessage(e)}`);
      await delay(o.reconnectDelayMs ?? 250, o.signal);
      continue;
    }
    if (Date.now() >= deadline) throw new Error("durable run observer timed out");
    await delay(o.pollMs ?? 400, o.signal);
  }
}

export function createRemoteBrowserExecutor({
  host,
  token,
  requiredCapabilities,
}: RemoteExecutorOptions) {
  let healthPromise: ReturnType<typeof checkRemoteHealth> | undefined;
  const ensureHealth = async (required: RemoteCapabilityRequirement[]) => {
    const h = await (healthPromise ??= checkRemoteHealth({ host, token }));
    if (!h.ok || !h.runtime || !h.manifest)
      throw new Error(
        `${h.error ?? "remote health handshake failed"}; upgrade oracle on the host and retry`,
      );
    const features = new Set(h.manifest.features.map((f) => `${f.id}@${f.version}`));
    for (const c of required)
      if (
        !features.has(`${c.id}@${c.version}`) ||
        (c.id === ARTIFACT_TRANSFER_FEATURE_ID && !h.capabilities?.artifactTransfer)
      )
        throw new Error(`Remote host does not support required capability ${c.id} v${c.version}`);
  };
  return async (options: BrowserRunOptions): Promise<BrowserRunResult> => {
    if (options.signal?.aborted)
      throw new Error("Remote browser run cancelled before the request was sent.");
    const captureOnly = options.config?.captureOnly === true;
    await ensureHealth([
      { id: DURABLE_QUEUE_FEATURE_ID, version: 1 },
      ...(requiredCapabilities ?? []),
      ...(captureOnly ? [{ id: CAPTURE_ONLY_FEATURE_ID, version: 1 }] : []),
    ]);
    const sessionId = options.sessionId ?? `remote-${randomBytes(12).toString("hex")}`;
    let receipt = await readDurableReceipt(sessionId);
    if (!receipt) {
      receipt = { sessionId, idempotencyKey: randomBytes(32).toString("hex") };
      await writeDurableReceipt(receipt);
    }
    const payload = await serializePayload(options, captureOnly);
    if (options.signal?.aborted) throw new Error("Remote browser run aborted before submission.");
    const payloadHash = createHash("sha256").update(JSON.stringify(payload)).digest("hex");
    if (receipt.payloadHash && receipt.payloadHash !== payloadHash)
      throw new Error("durable receipt payload does not match the current request");
    if (!receipt.payloadHash) {
      receipt = { ...receipt, payloadHash };
      await writeDurableReceipt(receipt);
    }
    let accepted: DurableRunSnapshot;
    if (receipt.runId) {
      accepted = await getDurableRemoteRun(host, receipt.runId, token);
    } else {
      if (options.signal?.aborted) throw new Error("Remote browser run aborted before submission.");
      try {
        accepted = await submitDurableRemoteRun({
          host,
          token,
          idempotencyKey: receipt.idempotencyKey,
          payload,
        });
      } catch (error) {
        if (options.signal?.aborted)
          throw new Error("Remote browser run aborted before submission.");
        if (!isRetryableTransport(error)) throw error;
        accepted = await submitDurableRemoteRun({
          host,
          token,
          idempotencyKey: receipt.idempotencyKey,
          payload,
        });
      }
      await writeDurableReceipt({ ...receipt, runId: accepted.id });
    }
    let cancelSent = false;
    const cancel = () => {
      if (!cancelSent) {
        cancelSent = true;
        void cancelDurableRemoteRun(host, accepted.id, token).catch(() => undefined);
      }
    };
    options.signal?.addEventListener("abort", cancel, { once: true });
    try {
      let w: DurableWatchOutcome;
      try { w = await watchDurableRemoteRun(host, accepted.id, {
        token,
        timeoutMs: Math.max(600_000, options.config?.timeoutMs ?? 0),
        signal: options.signal,
        onSnapshot: (s) => {
          options.log?.(`[remote] ${s.state} (${s.phase})`);
          const hint = (s as any).runtimeHint;
          if (hint && options.runtimeHintCb)
            void options.runtimeHintCb(hint, (hint as any).modelSelection);
        },
      }); } catch (error) { if (options.signal?.aborted) throw new Error("Remote browser run cancelled: the caller aborted."); throw error; }
      if (w.snapshot.state === "completed" && w.snapshot.result) {
        const raw = [
          ...(((w.snapshot as any).artifacts ?? []) as unknown[]),
          ...(((w.snapshot.result as any).artifacts ?? []) as unknown[]),
        ];
        const descriptors = raw.filter((x): x is RemoteArtifactDescriptor =>
          Boolean(x && typeof x === "object" && "artifactId" in x && "runId" in x),
        );
        const transferResults = await Promise.allSettled(descriptors.map((descriptor) => transferRemoteArtifact({ host, token, descriptor, sessionId, log: options.log })));
        const transferred = transferResults.flatMap((item) => item.status === "fulfilled" ? [item.value] : []);
        for (const failure of transferResults) if (failure.status === "rejected") options.log?.(`[remote] artifact transfer failed: ${safeMessage(failure.reason)}`);
        return {
          ...w.snapshot.result,
          savedFiles: [...(w.snapshot.result.savedFiles ?? []), ...transferred],
          artifacts: [...(w.snapshot.result.artifacts ?? []), ...transferred],
        };
      }
      throw new Error(w.snapshot.error ?? `remote durable run ended ${w.snapshot.state}`);
    } finally {
      options.signal?.removeEventListener("abort", cancel);
    }
  };
}
async function serializePayload(
  o: BrowserRunOptions,
  captureOnly: boolean,
): Promise<RemoteRunPayload> {
  const config = captureOnly
    ? Object.fromEntries(
        Object.entries(o.config ?? {}).filter(
          ([k]) => !["desiredModel", "modelStrategy", "thinkingTime", "researchMode"].includes(k),
        ),
      )
    : (o.config ?? {});
  return {
    prompt: captureOnly ? "" : o.prompt,
    attachments: captureOnly ? [] : await serializeAttachments(o.attachments ?? []),
    fallbackSubmission:
      !captureOnly && o.fallbackSubmission
        ? {
            prompt: o.fallbackSubmission.prompt,
            attachments: await serializeAttachments(o.fallbackSubmission.attachments ?? []),
          }
        : undefined,
    browserConfig: config,
    options: {
      heartbeatIntervalMs: o.heartbeatIntervalMs,
      verbose: o.verbose,
      sessionId: o.sessionId,
      followUpPrompts: captureOnly ? undefined : o.followUpPrompts,
    },
  };
}
async function serializeAttachments(a: BrowserAttachment[]): Promise<RemoteAttachmentPayload[]> {
  return Promise.all(
    a.map(async (x) => ({
      fileName: path.basename(x.path),
      displayPath: x.displayPath,
      sizeBytes: x.sizeBytes,
      contentBase64: (await readFile(x.path)).toString("base64"),
    })),
  );
}

export async function transferRemoteArtifact(p: {
  host: string;
  token?: string;
  descriptor: RemoteArtifactDescriptor;
  sessionId?: string;
  log?: (message: string) => void;
}): Promise<SavedBrowserFile> {
  const d = p.descriptor;
  validateArtifactDescriptor(d);
  const dir = resolveSessionArtifactsDir(p.sessionId ?? d.runId);
  await mkdir(dir, { recursive: true });
  const filename = sanitizeArtifactFilename(d.filename, `artifact-${d.artifactId}.bin`);
  const finalPath = await resolveUniqueArtifactPath(path.join(dir, filename));
  const part = `${finalPath}.part-${d.artifactId}`;
  try {
    await downloadArtifact(p.host, p.token, d, part);
    const s = await stat(part);
    if (s.size !== d.byteSize) throw new Error("artifact size mismatch");
    const sha256 = await computeFileSha256(part);
    if (sha256 !== d.sha256) throw new Error("artifact sha256 mismatch");
    const validation = await validateArtifactFile({
      path: part,
      filename,
      mimeType: sanitizeArtifactMimeType(d.mimeType),
    });
    if (!validation.ok) throw new Error(`${validation.type} validation failed`);
    await rename(part, finalPath);
    p.log?.(`[browser] Transferred artifact ${filename}`);
    return {
      kind: "file",
      path: finalPath,
      label: filename,
      filename,
      mimeType: sanitizeArtifactMimeType(d.mimeType),
      sizeBytes: s.size,
      sourceUrl: "bridge-artifact",
      finalUrl: "bridge-artifact",
      url: "bridge-artifact",
      sha256,
      validation,
      transfer: { status: "completed", bytes: s.size },
      origin: { mode: "bridge" },
    };
  } catch (e) {
    await rm(part, { force: true });
    throw e;
  }
}
function validateArtifactDescriptor(d: RemoteArtifactDescriptor): void {
  if (
    !d ||
    d.kind !== "file" ||
    !/^[a-zA-Z0-9_-]{1,128}$/.test(d.runId) ||
    !/^[a-zA-Z0-9_-]{1,128}$/.test(d.artifactId) ||
    !Number.isSafeInteger(d.byteSize) ||
    d.byteSize <= 0 ||
    d.byteSize > MAX_REMOTE_ARTIFACT_BYTES ||
    !/^[a-f0-9]{64}$/.test(d.sha256)
  )
    throw new Error("invalid bridge artifact descriptor");
}
async function downloadArtifact(
  host: string,
  token: string | undefined,
  d: RemoteArtifactDescriptor,
  target: string,
): Promise<void> {
  await new Promise((resolve, reject) => {
    const { hostname, port } = parseHostPort(host);
    const req = http.request(
      {
        hostname,
        port,
        path: `/runs/${encodeURIComponent(d.runId)}/artifacts/${encodeURIComponent(d.artifactId)}`,
        headers: token ? { authorization: `Bearer ${token}` } : undefined,
        timeout: DEFAULT_TIMEOUT_MS,
      },
      (res) => {
        if (res.statusCode !== 200) {
          res.resume();
          reject(new Error(`artifact download HTTP ${res.statusCode}`));
          return;
        }
        const out = createWriteStream(target, { flags: "wx" });
        let n = 0;
        const limit = new Transform({
          transform(chunk: Buffer, _e, cb) {
            n += chunk.length;
            cb(
              n > d.byteSize || n > MAX_REMOTE_ARTIFACT_BYTES
                ? new Error("artifact exceeds declared size")
                : null,
              chunk,
            );
          },
        });
        void pipeline(res, limit, out).then(resolve, reject);
      },
    );
    req.on("timeout", () => req.destroy(new Error("request timeout")));
    req.on("error", reject);
    req.end();
  });
}
function validateSnapshot(s: DurableRunSnapshot): void {
  if (
    !s ||
    typeof s !== "object" ||
    !/^[a-zA-Z0-9_-]{1,128}$/.test(s.id) ||
    !(["queued", "running", "completed", "failed", "canceled", "unknown"] as string[]).includes(
      s.state,
    ) ||
    typeof s.phase !== "string" ||
    !Number.isSafeInteger(s.queuePosition) ||
    !Number.isSafeInteger(s.roughEtaMs)
  )
    throw new Error("malformed durable run response");
}
function safeMessage(e: unknown): string {
  return (e instanceof Error ? e.message : String(e))
    .replace(/Bearer\s+[^\s)]+/gi, "Bearer [redacted]")
    .replace(/(authorization|token|api[_-]?key)\s*[:=]\s*[^,\s]+/gi, "$1=[redacted]");
}
function isRetryableTransport(e: unknown): boolean {
  return /ECONNRESET|ECONNREFUSED|EPIPE|socket hang up|request timeout|ETIMEDOUT|network/i.test(
    e instanceof Error ? e.message : String(e),
  );
}
function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const t = setTimeout(() => {
      settled = true;
      cleanup();
      resolve();
    }, ms);
    const onAbort = () => {
      if (settled) return;
      settled = true;
      clearTimeout(t);
      cleanup();
      reject(new Error("observer aborted"));
    };
    const cleanup = () => signal?.removeEventListener("abort", onAbort);
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) onAbort();
  });
}
async function requestDurableJson(p: {
  host: string;
  token?: string;
  method: string;
  path: string;
  idempotencyKey?: string;
  payload?: unknown;
  timeoutMs?: number;
}): Promise<any> {
  const { hostname, port } = parseHostPort(p.host);
  const body = p.payload === undefined ? undefined : Buffer.from(JSON.stringify(p.payload));
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname,
        port,
        path: p.path,
        method: p.method,
        timeout: p.timeoutMs ?? DEFAULT_TIMEOUT_MS,
        headers: {
          accept: "application/json",
          ...(body ? { "content-type": "application/json", "content-length": body.length } : {}),
          ...(p.idempotencyKey ? { "idempotency-key": p.idempotencyKey } : {}),
          ...(p.token ? { authorization: `Bearer ${p.token}` } : {}),
        },
      },
      (res) => {
        const chunks: Buffer[] = [];
        let bytes = 0;
        res.on("data", (c) => {
          bytes += Buffer.byteLength(c);
          if (bytes > 8 * 1024 * 1024) {
            req.destroy(new Error("remote response exceeds size limit"));
            return;
          }
          chunks.push(Buffer.from(c));
        });
        res.on("end", () => {
          let v: unknown;
          try {
            v = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          } catch {
            reject(new Error(`malformed remote response (HTTP ${res.statusCode})`));
            return;
          }
          if (!res.statusCode || res.statusCode < 200 || res.statusCode >= 300)
            reject(
              new Error(
                `remote request failed HTTP ${res.statusCode}: ${safeMessage((v as any)?.error ?? "request failed")}`,
              ),
            );
          else resolve(v);
        });
      },
    );
    req.on("timeout", () => req.destroy(new Error("request timeout")));
    req.on("error", (e) => reject(new Error(safeMessage(e))));
    if (body) req.write(body);
    req.end();
  });
}
