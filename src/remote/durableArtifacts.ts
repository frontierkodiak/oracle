import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { chmod, lstat, mkdir, open, rename, rm, stat } from "node:fs/promises";
import path from "node:path";
import type { BrowserRunResult } from "../browserMode.js";
import type { SessionArtifact } from "../sessionManager.js";
import type { RemoteArtifactDescriptor } from "./types.js";
import { MAX_REMOTE_ARTIFACT_BYTES } from "./types.js";

const MANIFEST = "manifest.json";
const ID_RE = /^[0-9a-f-]{36}$/i;

export interface DurableArtifactManifest {
  version: 1;
  runId: string;
  artifacts: RemoteArtifactDescriptor[];
  result: BrowserRunResult;
}

export interface DurableArtifactRun {
  runId: string;
  runRoot: string;
  manifestPath: string;
  descriptors: RemoteArtifactDescriptor[];
  /** Result with all local/ephemeral artifact references removed. */
  result: BrowserRunResult;
}

export async function persistBrowserRunArtifacts(params: {
  queueRoot: string;
  runId?: string;
  result: BrowserRunResult;
}): Promise<DurableArtifactRun> {
  const runId = params.runId ?? randomUUID();
  if (!ID_RE.test(runId)) throw new Error("invalid run id");
  await ensurePrivateDirectory(params.queueRoot, true);
  const runsRoot = path.join(params.queueRoot, "runs");
  const runRoot = path.join(runsRoot, runId);
  const artifactsRoot = path.join(runRoot, "artifacts");
  await ensurePrivateDirectory(runsRoot, true);
  await ensurePrivateDirectory(runRoot, true);
  const stagingRoot = path.join(runRoot, `.artifacts-${randomUUID()}`);
  await ensurePrivateDirectory(stagingRoot, false);
  const existingArtifacts = await lstat(artifactsRoot).catch(() => undefined);
  if (existingArtifacts) {
    await rm(stagingRoot, { recursive: true, force: true });
    throw new Error("artifact directory already exists; refusing stale reuse");
  }
  await rename(stagingRoot, artifactsRoot);
  await chmod(artifactsRoot, 0o700);
  const descriptors: RemoteArtifactDescriptor[] = [];
  const seen = new Set<string>();
  try {
    for (const artifact of localArtifacts(params.result)) {
      if (seen.has(artifact.path)) continue;
      seen.add(artifact.path);
      const descriptor = await copyArtifact({ artifact, runId, artifactsRoot });
      descriptors.push(descriptor);
    }
    const artifactsHandle = await open(artifactsRoot, constants.O_RDONLY);
    try {
      await artifactsHandle.sync();
    } finally {
      await artifactsHandle.close();
    }
    const manifest: DurableArtifactManifest = {
      version: 1,
      runId,
      artifacts: descriptors,
      result: sanitizeResult(params.result),
    };
    await atomicWriteJson(path.join(runRoot, MANIFEST), manifest);
    return {
      runId,
      runRoot,
      manifestPath: path.join(runRoot, MANIFEST),
      descriptors,
      result: sanitizeResult(params.result),
    };
  } catch (error) {
    await rm(artifactsRoot, { recursive: true, force: true }).catch(() => undefined);
    await rm(path.join(runRoot, MANIFEST), { force: true }).catch(() => undefined);
    throw error;
  }
}

export async function reopenDurableArtifactRun(params: {
  queueRoot: string;
  runId: string;
}): Promise<DurableArtifactRun> {
  if (!ID_RE.test(params.runId)) throw new Error("invalid run id");
  const runRoot = path.join(params.queueRoot, "runs", params.runId);
  const runInfo = await lstat(runRoot);
  if (!runInfo.isDirectory() || runInfo.isSymbolicLink() || (runInfo.mode & 0o777) !== 0o700)
    throw new Error("unsafe artifact run directory");
  const manifestPath = path.join(runRoot, MANIFEST);
  const manifestInfo = await lstat(manifestPath);
  if (
    !manifestInfo.isFile() ||
    manifestInfo.isSymbolicLink() ||
    (manifestInfo.mode & 0o777) !== 0o600
  )
    throw new Error("unsafe artifact manifest");
  const manifestHandle = await open(manifestPath, constants.O_RDONLY | constants.O_NOFOLLOW);
  let manifest: DurableArtifactManifest;
  try {
    manifest = parseManifest(JSON.parse(await manifestHandle.readFile("utf8")));
  } finally {
    await manifestHandle.close();
  }
  if (manifest.runId !== params.runId) throw new Error("manifest run id mismatch");
  const artifactsRoot = path.join(runRoot, "artifacts");
  const rootStat = await lstat(artifactsRoot);
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink() || (rootStat.mode & 0o777) !== 0o700)
    throw new Error("artifact directory is not safe");
  for (const descriptor of manifest.artifacts)
    await verifyDescriptor(artifactsRoot, descriptor, params.runId);
  return {
    runId: params.runId,
    runRoot,
    manifestPath,
    descriptors: manifest.artifacts,
    result: manifest.result,
  };
}

export async function resolveDurableArtifact(params: {
  queueRoot: string;
  runId: string;
  artifactId: string;
}): Promise<{ descriptor: RemoteArtifactDescriptor; filePath: string }> {
  const run = await reopenDurableArtifactRun(params);
  const descriptor = run.descriptors.find((item) => item.artifactId === params.artifactId);
  if (!descriptor) throw new Error("artifact not found");
  return {
    descriptor,
    filePath: path.join(
      run.runRoot,
      "artifacts",
      descriptor.artifactId + "-" + descriptor.filename,
    ),
  };
}

function localArtifacts(result: BrowserRunResult): SessionArtifact[] {
  return [
    ...(result.artifacts ?? []),
    ...(result.savedImages ?? []),
    ...(result.savedFiles ?? []),
  ].filter(
    (a) =>
      a?.path &&
      (a.kind === "file" ||
        a.kind === "image" ||
        a.kind === "transcript" ||
        a.kind === "deep-research-report"),
  );
}

async function copyArtifact(params: {
  artifact: SessionArtifact;
  runId: string;
  artifactsRoot: string;
}): Promise<RemoteArtifactDescriptor> {
  const source = params.artifact.path;
  const sourceStat = await lstat(source);
  if (!sourceStat.isFile() || sourceStat.isSymbolicLink())
    throw new Error("artifact source is not a regular file");
  if (sourceStat.size <= 0) throw new Error("artifact is empty");
  if (sourceStat.size > MAX_REMOTE_ARTIFACT_BYTES || source.endsWith(".crdownload"))
    throw new Error("artifact exceeds transfer policy or is incomplete");
  const filename = sanitizeFilename(path.basename((params.artifact as any).filename ?? params.artifact.label ?? source), "artifact.bin");
  const artifactId = randomUUID();
  const destination = path.join(params.artifactsRoot, `${artifactId}-${filename}`);
  const input = await open(source, constants.O_RDONLY | constants.O_NOFOLLOW);
  let copiedHash = "";
  try {
    const openedStat = await input.stat();
    if (!openedStat.isFile() || openedStat.size <= 0)
      throw new Error("artifact source changed to nonregular or empty");
    const output = await open(
      destination,
      constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
      0o600,
    );
    try {
      const buffer = Buffer.allocUnsafe(1024 * 1024);
      let total = 0;
      const hash = createHash("sha256");
      for (;;) {
        const { bytesRead } = await input.read(buffer, 0, buffer.length, null);
        if (!bytesRead) break;
        total += bytesRead;
        if (total > MAX_REMOTE_ARTIFACT_BYTES) throw new Error("artifact exceeds transfer limit");
        const chunk = buffer.subarray(0, bytesRead);
        hash.update(chunk);
        await writeFully(output, chunk);
      }
      if (total !== openedStat.size) throw new Error("artifact changed during copy");
      copiedHash = hash.digest("hex");
      await output.sync();
    } finally {
      await output.close();
    }
  } finally {
    await input.close();
  }
  await chmod(destination, 0o600);
  const copied = await stat(destination);
  const sha256 = copiedHash;
  return {
    artifactId,
    runId: params.runId,
    kind: "file",
    filename,
    mimeType: params.artifact.mimeType,
    byteSize: copied.size,
    sha256,
    validation: { type: "generic", ok: true },
    sourceUrlKind: sourceKind(params.artifact.sourceUrl),
    transferStatus: "ready",
  };
}

async function writeFully(handle: Awaited<ReturnType<typeof open>>, chunk: Buffer): Promise<void> {
  let offset = 0;
  while (offset < chunk.length) {
    const { bytesWritten } = await handle.write(chunk, offset, chunk.length - offset);
    if (bytesWritten <= 0) throw new Error("artifact write made no progress");
    offset += bytesWritten;
  }
}

async function verifyDescriptor(
  root: string,
  descriptor: RemoteArtifactDescriptor,
  runId: string,
): Promise<void> {
  if (
    !ID_RE.test(descriptor.artifactId) ||
    descriptor.runId !== runId ||
    descriptor.kind !== "file" ||
    descriptor.filename !== sanitizeFilename(descriptor.filename, "artifact.bin")
  )
    throw new Error("invalid artifact descriptor");
  const filePath = path.join(root, descriptor.artifactId + "-" + descriptor.filename);
  const item = await lstat(filePath);
  if (
    !item.isFile() ||
    item.isSymbolicLink() ||
    (item.mode & 0o777) !== 0o600 ||
    item.size !== descriptor.byteSize
  )
    throw new Error("artifact identity or size mismatch");
  if ((await hashFile(filePath)) !== descriptor.sha256) throw new Error("artifact hash mismatch");
}

function parseManifest(value: unknown): DurableArtifactManifest {
  if (!value || typeof value !== "object") throw new Error("invalid artifact manifest");
  const item = value as Record<string, unknown>;
  if (
    Object.keys(item).sort().join(",") !== "artifacts,result,runId,version" ||
    item.version !== 1 ||
    typeof item.runId !== "string" ||
    !ID_RE.test(item.runId) ||
    !Array.isArray(item.artifacts) ||
    !item.result ||
    typeof item.result !== "object"
  )
    throw new Error("invalid artifact manifest");
  for (const descriptor of item.artifacts) validateDescriptor(descriptor, item.runId);
  const result = item.result as Record<string, unknown>;
  if (
    typeof result.answerText !== "string" ||
    typeof result.answerMarkdown !== "string" ||
    typeof result.tookMs !== "number" ||
    typeof result.answerTokens !== "number" ||
    typeof result.answerChars !== "number" ||
    Object.keys(result).some(
      (key) =>
        [
          "answerText",
          "answerMarkdown",
          "answerHtml",
          "tookMs",
          "answerTokens",
          "answerChars",
          "browserTransport",
          "conversationId",
          "promptSubmitted",
          "warnings",
          "archive",
          "modelSelection",
          "thinkingSelection",
          "tabUrl",
        ].includes(key) === false,
    )
  )
    throw new Error("invalid sanitized result");
  return {
    version: 1,
    runId: item.runId,
    artifacts: item.artifacts as RemoteArtifactDescriptor[],
    result: item.result as BrowserRunResult,
  };
}
function validateDescriptor(
  value: unknown,
  runId: string,
): asserts value is RemoteArtifactDescriptor {
  if (!value || typeof value !== "object") throw new Error("invalid artifact descriptor");
  const d = value as Record<string, unknown>;
  const keys = Object.keys(d).sort().join(",");
  if (
    keys !==
      "artifactId,byteSize,filename,kind,runId,sha256,sourceUrlKind,transferStatus,validation" &&
    keys !==
      "artifactId,byteSize,filename,kind,mimeType,runId,sha256,sourceUrlKind,transferStatus,validation"
  )
    throw new Error("invalid artifact descriptor");
  if (
    typeof d.artifactId !== "string" ||
    !ID_RE.test(d.artifactId) ||
    d.runId !== runId ||
    d.kind !== "file" ||
    typeof d.filename !== "string" ||
    d.filename !== sanitizeFilename(d.filename, "artifact.bin") ||
    typeof d.byteSize !== "number" ||
    !Number.isSafeInteger(d.byteSize) ||
    d.byteSize <= 0 ||
    typeof d.sha256 !== "string" ||
    !/^[a-f0-9]{64}$/.test(d.sha256) ||
    !["sandbox", "chatgpt-file-endpoint", "browser-download"].includes(String(d.sourceUrlKind)) ||
    !["ready", "streaming", "completed", "failed", "skipped"].includes(String(d.transferStatus))
  )
    throw new Error("invalid artifact descriptor");
  if (d.mimeType !== undefined && typeof d.mimeType !== "string")
    throw new Error("invalid artifact mime");
  if (
    !d.validation ||
    typeof d.validation !== "object" ||
    !["generic", "zip"].includes(String((d.validation as any).type)) ||
    typeof (d.validation as any).ok !== "boolean" ||
    Object.keys(d.validation as object).some((key) => !["type", "ok", "error"].includes(key)) ||
    ((d.validation as any).error !== undefined && typeof (d.validation as any).error !== "string")
  )
    throw new Error("invalid artifact validation");
}
async function ensurePrivateDirectory(dir: string, allowExisting: boolean): Promise<void> {
  const current = await lstat(dir).catch(() => undefined);
  if (current) {
    if (!allowExisting || !current.isDirectory() || current.isSymbolicLink())
      throw new Error(`unsafe queue directory: ${dir}`);
    await chmod(dir, 0o700);
    return;
  }
  await mkdir(dir, { recursive: false, mode: 0o700 });
  await chmod(dir, 0o700);
}
async function hashFile(filePath: string): Promise<string> {
  const hash = createHash("sha256");
  const input = await open(filePath, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const buffer = Buffer.allocUnsafe(1024 * 1024);
    for (;;) {
      const { bytesRead } = await input.read(buffer, 0, buffer.length, null);
      if (!bytesRead) break;
      hash.update(buffer.subarray(0, bytesRead));
    }
  } finally {
    await input.close();
  }
  return hash.digest("hex");
}
async function atomicWriteJson(filePath: string, value: unknown): Promise<void> {
  const temp = `${filePath}.${randomUUID()}.tmp`;
  const handle = await open(temp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL, 0o600);
  try {
    try {
      await handle.writeFile(JSON.stringify(value) + "\n");
      await handle.sync();
    } finally {
      await handle.close();
    }
    await rename(temp, filePath);
    await chmod(filePath, 0o600);
    const directory = await open(path.dirname(filePath), constants.O_RDONLY);
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
  } catch (error) {
    await rm(temp, { force: true }).catch(() => undefined);
    throw error;
  }
}
function sanitizeFilename(raw: string, fallback: string): string {
  const name = path
    .basename(raw)
    .replace(/[^a-zA-Z0-9._-]/g, "_")
    .replace(/^\.+$/, "");
  return name || fallback;
}
function sourceKind(source?: string): RemoteArtifactDescriptor["sourceUrlKind"] {
  if (source?.startsWith("sandbox:")) return "sandbox";
  if (source === "browser-download") return "browser-download";
  return "chatgpt-file-endpoint";
}
function sanitizeResult(result: BrowserRunResult): BrowserRunResult {
  return {
    answerText: result.answerText,
    answerMarkdown: result.answerMarkdown,
    answerHtml: result.answerHtml,
    tookMs: result.tookMs,
    answerTokens: result.answerTokens,
    answerChars: result.answerChars,
    modelSelection: result.modelSelection,
    thinkingSelection: result.thinkingSelection,
    archive: result.archive
      ? {
          mode: result.archive.mode,
          attempted: result.archive.attempted,
          archived: result.archive.archived,
          reason: result.archive.reason,
          conversationUrl: result.archive.conversationUrl,
        }
      : undefined,
    tabUrl: result.tabUrl,
    conversationId: result.conversationId,
    promptSubmitted: result.promptSubmitted,
    warnings: result.warnings?.map(({ code, severity, message }) => ({ code, severity, message: message.replace(/(?:\/Users\/|[A-Za-z]:\\|\/home\/)[^\s)]+/g, "[redacted-path]") })),
  };
}
