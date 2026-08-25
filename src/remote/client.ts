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
import type { BrowserRunWarning } from "../sessionManager.js";
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
  submission?: "unknown";
}
export class DurableSubmissionUnknownError extends Error {
  readonly reconnectable = true;
  constructor(message = "durable run submission outcome is unknown; retry with the saved key") {
    super(message);
    this.name = "DurableSubmissionUnknownError";
  }
}
class RemoteTransportError extends Error {
  constructor(
    message: string,
    readonly phase: "pre-submit" | "post-submit",
  ) {
    super(message);
    this.name = "RemoteTransportError";
  }
}
export class RemoteArtifactWarning extends Error {
  readonly warning: BrowserRunWarning;
  constructor(artifactId: string, message: string) {
    super(message);
    this.name = "RemoteArtifactWarning";
    this.warning = {
      code: "remote-artifact-transfer-failed",
      severity: "warning",
      message,
      details: { artifactId },
    };
  }
}
export interface DurableWatchOptions {
  token?: string;
  signal?: AbortSignal;
  timeoutMs?: number;
  reconnectDelayMs?: number;
  pollMs?: number;
  onSnapshot?: (snapshot: DurableRunSnapshot) => void;
  onEvent?: (event: unknown) => void;
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
  await ensurePrivateTree(path.dirname(target), root, "unsafe durable receipt directory");
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
    } finally {
      await handle.close();
    }
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
    Object.keys(r).some(
      (key) => !["sessionId", "idempotencyKey", "runId", "payloadHash", "submission"].includes(key),
    ) ||
    r.sessionId !== sessionId ||
    typeof r.idempotencyKey !== "string" ||
    !/^[a-f0-9]{64}$/.test(r.idempotencyKey) ||
    (r.runId !== undefined &&
      (typeof r.runId !== "string" || !/^[a-zA-Z0-9_-]{1,128}$/.test(r.runId))) ||
    (r.payloadHash !== undefined &&
      (typeof r.payloadHash !== "string" || !/^[a-f0-9]{64}$/.test(r.payloadHash))) ||
    (r.submission !== undefined && r.submission !== "unknown")
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
    if (
      (e as any).seq <= prior ||
      !Number.isSafeInteger((e as any).seq) ||
      !isKnownRemoteEvent((e as any).event)
    )
      throw new Error("malformed or nonmonotonic durable events response");
    prior = (e as any).seq;
  }
  return v.events;
}
function isKnownRemoteEvent(value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const e = value as Record<string, unknown>;
  const keys = (allowed: string[]) => Object.keys(e).every((key) => allowed.includes(key));
  if (e.type === "accepted") return keys(["type"]);
  if (e.type === "log" || e.type === "error")
    return keys(["type", "message"]) && typeof e.message === "string";
  if (e.type === "state")
    return (
      keys(["type", "state", "phase"]) &&
      ["queued", "running", "completed", "failed", "canceled", "unknown"].includes(
        String(e.state),
      ) &&
      [
        "accepted",
        "dispatching",
        "browser_attached",
        "prompt_submitted",
        "awaiting_response",
        "capturing",
        "terminal",
      ].includes(String(e.phase))
    );
  if (e.type === "cancellation") return keys(["type", "outcome"]) && typeof e.outcome === "string";
  if (e.type === "result")
    return keys(["type", "result"]) && Boolean(e.result && typeof e.result === "object");
  if (e.type === "artifact-ready")
    return (
      typeof e.runId === "string" &&
      (() => {
        try {
          validateArtifactDescriptor(e.artifact as RemoteArtifactDescriptor);
          return true;
        } catch {
          return false;
        }
      })()
    );
  if (e.type === "artifact-progress")
    return (
      keys(["type", "artifactId", "receivedBytes", "totalBytes", "phase"]) &&
      typeof e.artifactId === "string" &&
      ["download", "transfer", "validate"].includes(String(e.phase)) &&
      [e.receivedBytes, e.totalBytes].every(
        (n) => n === undefined || (Number.isSafeInteger(n) && Number(n) >= 0),
      )
    );
  return false;
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
      for (const e of events) {
        after = Math.max(after, e.seq);
        o.onEvent?.(e.event);
      }
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
    if (!h.ok || !h.runtime || !h.manifest) {
      const detail = h.error ?? "remote health handshake failed";
      if (!h.statusCode && /ECONNREFUSED|ENOTFOUND|EHOSTUNREACH|ETIMEDOUT/.test(detail))
        throw new Error(`Could not reach the research bridge at ${host} (${detail}).`, {
          cause: new Error(detail),
        });
      throw new Error(`${detail}; upgrade oracle on the host and retry`);
    }
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
        if (!isRetryableTransport(error) || isDefinitePreSubmit(error)) throw error;
        try {
          accepted = await submitDurableRemoteRun({
            host,
            token,
            idempotencyKey: receipt.idempotencyKey,
            payload,
          });
        } catch (retryError) {
          if (isRetryableTransport(retryError) && !isDefinitePreSubmit(retryError)) {
            await writeDurableReceipt({ ...receipt, submission: "unknown" });
            throw new DurableSubmissionUnknownError();
          }
          throw retryError;
        }
      }
      await writeDurableReceipt({ ...receipt, runId: accepted.id, submission: undefined });
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
      try {
        w = await watchDurableRemoteRun(host, accepted.id, {
          token,
          timeoutMs: options.config?.timeoutMs ?? 600_000,
          signal: options.signal,
          onSnapshot: (s) => {
            options.log?.(`[remote] ${s.state} (${s.phase})`);
            const hint = (s as any).runtimeHint;
            if (hint && options.runtimeHintCb)
              void options.runtimeHintCb(hint, (hint as any).modelSelection);
          },
          onEvent: (event) => {
            if (event && typeof event === "object" && (event as any).type === "log")
              options.log?.(String((event as any).message ?? ""));
          },
        });
      } catch (error) {
        if (options.signal?.aborted)
          throw new Error("Remote browser run cancelled: the caller aborted.");
        throw error;
      }
      if (w.snapshot.state === "completed" && w.snapshot.result) {
        const raw = [
          ...(((w.snapshot as any).artifacts ?? []) as unknown[]),
          ...(((w.snapshot.result as any).artifacts ?? []) as unknown[]),
        ];
        const descriptors = raw.filter((x): x is RemoteArtifactDescriptor =>
          Boolean(x && typeof x === "object" && "artifactId" in x && "runId" in x),
        );
        const transferResults: PromiseSettledResult<SavedBrowserFile>[] = [];
        for (const descriptor of descriptors) {
          try {
            transferResults.push({
              status: "fulfilled",
              value: await transferRemoteArtifact({
                host,
                token,
                descriptor,
                sessionId,
                log: options.log,
              }),
            });
          } catch (reason) {
            transferResults.push({ status: "rejected", reason });
          }
        }
        const transferred = transferResults.flatMap((item) =>
          item.status === "fulfilled" ? [item.value] : [],
        );
        const transferWarnings = transferResults.flatMap((item, index) =>
          item.status === "rejected"
            ? [
                new RemoteArtifactWarning(
                  descriptors[index]?.artifactId ?? "unknown",
                  `Artifact ${descriptors[index]?.artifactId ?? "unknown"} transfer failed: ${safeMessage(item.reason)}`,
                ).warning,
              ]
            : [],
        );
        for (const warning of transferWarnings) options.log?.(`[remote] ${warning.message}`);
        return {
          ...w.snapshot.result,
          savedFiles:
            transferred.length || w.snapshot.result.savedFiles?.length
              ? [...(w.snapshot.result.savedFiles ?? []), ...transferred]
              : undefined,
          artifacts: transferred.length ? transferred : undefined,
          warnings: [...(w.snapshot.result.warnings ?? []), ...transferWarnings],
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
  await ensurePrivateTree(
    dir,
    path.resolve(getOracleHomeDir()),
    "unsafe durable artifact directory",
  );
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
    !/^[a-f0-9]{64}$/.test(d.sha256) ||
    typeof d.filename !== "string" ||
    d.filename.length === 0 ||
    d.filename.length > 255 ||
    !["sandbox", "chatgpt-file-endpoint", "browser-download"].includes(d.sourceUrlKind) ||
    !["ready", "streaming", "completed", "failed", "skipped"].includes(d.transferStatus)
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
        const out = createWriteStream(target, {
          flags: (fsConstants.O_WRONLY |
            fsConstants.O_CREAT |
            fsConstants.O_EXCL |
            fsConstants.O_NOFOLLOW) as unknown as string,
          mode: 0o600,
        });
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
    s &&
    typeof s === "object" &&
    Object.keys(s).some(
      (key) =>
        ![
          "id",
          "state",
          "phase",
          "queuePosition",
          "roughEtaMs",
          "createdAt",
          "updatedAt",
          "requestHash",
          "runtimeHint",
          "modelSelection",
          "artifacts",
          "failure",
          "cancellation",
          "result",
          "error",
          "errorMetadata",
        ].includes(key),
    )
  )
    throw new Error("malformed durable run response");
  if (
    !s ||
    typeof s !== "object" ||
    !/^[a-zA-Z0-9_-]{1,128}$/.test(s.id) ||
    !(["queued", "running", "completed", "failed", "canceled", "unknown"] as string[]).includes(
      s.state,
    ) ||
    ![
      "accepted",
      "dispatching",
      "browser_attached",
      "prompt_submitted",
      "awaiting_response",
      "capturing",
      "terminal",
    ].includes(s.phase) ||
    typeof s.requestHash !== "string" ||
    !/^[a-f0-9]{64}$/.test(s.requestHash) ||
    !Number.isSafeInteger(s.queuePosition) ||
    !Number.isSafeInteger(s.roughEtaMs) ||
    s.queuePosition < 0 ||
    s.roughEtaMs < 0 ||
    !isIsoTimestamp(s.createdAt) ||
    !isIsoTimestamp(s.updatedAt) ||
    new Date(s.updatedAt).getTime() < new Date(s.createdAt).getTime()
  )
    throw new Error("malformed durable run response");
  if (s.state === "completed" && (!s.result || !isValidResult(s.result)))
    throw new Error("malformed completed durable run response");
  if (s.state === "failed" && typeof s.error !== "string" && !s.failure)
    throw new Error("malformed failed durable run response");
  if (s.state === "canceled" && !s.cancellation)
    throw new Error("malformed canceled durable run response");
  if (s.state === "unknown" && typeof s.error !== "string" && !s.failure)
    throw new Error("malformed unknown durable run response");
  if (
    s.runtimeHint !== undefined &&
    (!s.runtimeHint || typeof s.runtimeHint !== "object" || Array.isArray(s.runtimeHint))
  )
    throw new Error("malformed durable runtime hint");
  for (const field of ["failure", "errorMetadata"] as const) {
    if (s[field] !== undefined && !isFailureMetadata(s[field]))
      throw new Error(`malformed durable ${field}`);
  }
  if (
    s.cancellation !== undefined &&
    (!s.cancellation ||
      typeof s.cancellation !== "object" ||
      (s.cancellation.requestedAt !== undefined && !isIsoTimestamp(s.cancellation.requestedAt)) ||
      (s.cancellation.outcome !== undefined && typeof s.cancellation.outcome !== "string"))
  )
    throw new Error("malformed durable cancellation");
  if (
    s.artifacts !== undefined &&
    (!Array.isArray(s.artifacts) ||
      s.artifacts.some((a) => {
        try {
          validateArtifactDescriptor(a);
          return false;
        } catch {
          return true;
        }
      }))
  )
    throw new Error("malformed durable artifact descriptors");
}
function isValidResult(result: BrowserRunResult): boolean {
  return (
    typeof result.answerText === "string" &&
    typeof result.answerMarkdown === "string" &&
    [result.tookMs, result.answerTokens, result.answerChars].every(
      (n) => Number.isSafeInteger(n) && n >= 0,
    )
  );
}
function isFailureMetadata(value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const r = value as Record<string, unknown>;
  if (Object.keys(r).some((key) => !["code", "type", "throttleMs", "message"].includes(key)))
    return false;
  return (
    [r.code, r.type, r.message].every((v) => v === undefined || typeof v === "string") &&
    (r.throttleMs === undefined ||
      (Number.isSafeInteger(r.throttleMs) && Number(r.throttleMs) >= 0))
  );
}
function isIsoTimestamp(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z$/.test(value) &&
    Number.isFinite(Date.parse(value))
  );
}
async function ensurePrivateTree(dir: string, root: string, message: string): Promise<void> {
  const resolvedRoot = path.resolve(root);
  const resolvedDir = path.resolve(dir);
  if (resolvedDir !== resolvedRoot && !resolvedDir.startsWith(resolvedRoot + path.sep))
    throw new Error(message);
  const relative = path.relative(resolvedRoot, resolvedDir);
  try {
    await mkdir(resolvedRoot, { mode: 0o700 });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  }
  let current = resolvedRoot;
  for (const segment of relative ? relative.split(path.sep) : []) {
    current = path.join(current, segment);
    try {
      await mkdir(current, { mode: 0o700 });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    const info = await lstat(current);
    if (!info.isDirectory() || info.isSymbolicLink()) throw new Error(message);
    await chmod(current, 0o700);
  }
  const rootInfo = await lstat(resolvedRoot);
  if (!rootInfo.isDirectory() || rootInfo.isSymbolicLink()) throw new Error(message);
  await chmod(resolvedRoot, 0o700);
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
function isDefinitePreSubmit(e: unknown): boolean {
  return e instanceof RemoteTransportError && e.phase === "pre-submit";
}
function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    let connected = false;
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
    let connected = false;
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
    req.on("socket", (socket) =>
      socket.once("connect", () => {
        connected = true;
      }),
    );
    req.on("timeout", () =>
      req.destroy(
        new RemoteTransportError("request timeout", connected ? "post-submit" : "pre-submit"),
      ),
    );
    req.on("error", (e) =>
      reject(new RemoteTransportError(safeMessage(e), connected ? "post-submit" : "pre-submit")),
    );
    if (body) req.write(body);
    req.end();
  });
}
