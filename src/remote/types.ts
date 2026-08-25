import type { BrowserSessionConfig } from "../sessionStore.js";
import type { BrowserRunResult } from "../browserMode.js";
import type { BrowserAttachment } from "../browser/types.js";
import type { SessionArtifactValidation } from "../sessionManager.js";

export const MAX_REMOTE_ARTIFACT_BYTES = 512 * 1024 * 1024;
export const REMOTE_HEALTH_SCHEMA_VERSION = 1;
export const ARTIFACT_TRANSFER_FEATURE_ID = "oracle.remote.artifact-transfer";
export const CAPTURE_ONLY_FEATURE_ID = "oracle.browser.capture-only";
export const DURABLE_QUEUE_FEATURE_ID = "oracle.remote.durable-queue";

/** Browser settings a remote caller may place on the transport wire. */
export const CLIENT_BROWSER_CONFIG_FIELDS = [
  "chatgptUrl",
  "url",
  "desiredModel",
  "modelStrategy",
  "thinkingTime",
  "researchMode",
  "archiveConversations",
  "resumeConversationUrl",
  "captureProviderNative",
  "captureOnly",
  "timeoutMs",
  "inputTimeoutMs",
  "attachmentTimeoutMs",
  "assistantRecheckDelayMs",
  "assistantRecheckTimeoutMs",
  "autoReattachDelayMs",
  "autoReattachIntervalMs",
  "autoReattachTimeoutMs",
  "keepBrowser",
  "debug",
] as const satisfies readonly (keyof BrowserSessionConfig)[];

/**
 * Whitelist rather than blacklist: profile paths, cookies, debugger targets,
 * executable paths, tab selectors, and host concurrency remain host-owned.
 */
export function pickClientBrowserConfig(
  requested: BrowserSessionConfig | undefined | null,
): BrowserSessionConfig {
  const accepted: BrowserSessionConfig = {};
  if (!requested) return accepted;
  for (const field of CLIENT_BROWSER_CONFIG_FIELDS) {
    const value = requested[field];
    if (value !== undefined) (accepted as Record<string, unknown>)[field] = value;
  }
  return accepted;
}

const REMOTE_HOST_PRIVATE_RESULT_KEYS = new Set([
  "path",
  "filePath",
  "localPath",
  "chromePid",
  "chromePort",
  "chromeHost",
  "chromeBrowserWSEndpoint",
  "chromeProfileRoot",
  "userDataDir",
  "chromeTargetId",
  "controllerPid",
  "debugPort",
  "remoteChrome",
  "chromePath",
  "chromeProfile",
  "chromeCookiePath",
  "copyProfileSource",
  "inlineCookies",
  "inlineCookiesSource",
  "browserTabRef",
]);

export function isRemoteHostPrivateResultKey(key: string): boolean {
  return REMOTE_HOST_PRIVATE_RESULT_KEYS.has(key);
}

/** Remove host-local process, profile, cookie, debugger, and filesystem state
 * before a browser value crosses the remote transport boundary. */
export function sanitizeRemotePublicValue<T>(value: T): T {
  if (Array.isArray(value)) return value.map(sanitizeRemotePublicValue) as T;
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value)
      .filter(([key]) => !isRemoteHostPrivateResultKey(key))
      .map(([key, child]) => [key, sanitizeRemotePublicValue(child)]),
  ) as T;
}

export function isRemotePublicValue(value: unknown): boolean {
  if (Array.isArray(value)) return value.every(isRemotePublicValue);
  if (!value || typeof value !== "object") return true;
  return Object.entries(value).every(
    ([key, child]) => !isRemoteHostPrivateResultKey(key) && isRemotePublicValue(child),
  );
}

const REMOTE_RUNTIME_HINT_FIELDS = [
  "tabUrl",
  "conversationId",
  "submissionAttempted",
  "promptSubmitted",
  "modelSelection",
] as const;

/** Runtime hints are public queue state, so expose only conversation identity and
 * submission/model evidence. Chrome control handles remain host-private. */
export function sanitizeRemoteRuntimeHint(
  hint: Record<string, unknown>,
  modelSelection?: unknown,
): Record<string, unknown> {
  return {
    ...(typeof hint.tabUrl === "string" ? { tabUrl: hint.tabUrl } : {}),
    ...(typeof hint.conversationId === "string" ? { conversationId: hint.conversationId } : {}),
    ...(typeof hint.submissionAttempted === "boolean"
      ? { submissionAttempted: hint.submissionAttempted }
      : {}),
    ...(typeof hint.promptSubmitted === "boolean" ? { promptSubmitted: hint.promptSubmitted } : {}),
    ...(modelSelection === undefined
      ? {}
      : { modelSelection: sanitizeRemotePublicValue(modelSelection) }),
  };
}

export function isRemoteRuntimeHint(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const hint = value as Record<string, unknown>;
  if (Object.keys(hint).some((key) => !REMOTE_RUNTIME_HINT_FIELDS.includes(key as never)))
    return false;
  return (
    (hint.tabUrl === undefined || typeof hint.tabUrl === "string") &&
    (hint.conversationId === undefined || typeof hint.conversationId === "string") &&
    (hint.submissionAttempted === undefined || typeof hint.submissionAttempted === "boolean") &&
    (hint.promptSubmitted === undefined || typeof hint.promptSubmitted === "boolean") &&
    (hint.modelSelection === undefined || isRemotePublicValue(hint.modelSelection))
  );
}

const REMOTE_PUBLIC_LOG_MESSAGES = new Set([
  "Uploading attachment",
  "Attachments uploaded",
  "Prompt ready",
  "Prompt dispatch attempted",
  "Prompt dispatched",
  "Waiting for response",
  "Capturing response",
  "Browser progress updated",
]);

/** Browser logging is rich in local paths and Chrome handles. Convert it to a
 * small public vocabulary rather than trying to redact arbitrary prose. */
export function summarizeRemoteBrowserLog(message: string): string {
  if (/uploading attachment/i.test(message)) return "Uploading attachment";
  if (/attachments? uploaded|upload.*complete/i.test(message)) return "Attachments uploaded";
  if (/prompt textarea ready|composer.*ready/i.test(message)) return "Prompt ready";
  if (/submit.*attempt/i.test(message)) return "Prompt dispatch attempted";
  if (/submitted prompt|clicked send button/i.test(message)) return "Prompt dispatched";
  if (/waiting.*(?:response|assistant)|thinking/i.test(message)) return "Waiting for response";
  if (/captur|harvest/i.test(message)) return "Capturing response";
  return "Browser progress updated";
}

export function isRemotePublicLogMessage(value: unknown): value is string {
  return typeof value === "string" && REMOTE_PUBLIC_LOG_MESSAGES.has(value);
}

export interface RemoteCapabilityFeature {
  id: string;
  version: number;
  limits?: Record<string, unknown>;
}

export interface RemoteCapabilityRequirement {
  id: string;
  version: number;
}

export interface RemoteCapabilityManifest {
  schemaVersion: 1;
  features: RemoteCapabilityFeature[];
}

export interface RemoteAttachmentPayload {
  fileName: string;
  displayPath: string;
  sizeBytes?: number;
  contentBase64: string;
}

export interface RemoteRunPayload {
  prompt: string;
  attachments: RemoteAttachmentPayload[];
  fallbackSubmission?: {
    prompt: string;
    attachments: RemoteAttachmentPayload[];
  };
  browserConfig: BrowserSessionConfig;
  options: {
    heartbeatIntervalMs?: number;
    verbose?: boolean;
    sessionId?: string;
    followUpPrompts?: string[];
  };
}

export interface DurableRunSnapshot {
  id: string;
  state: "queued" | "running" | "completed" | "failed" | "canceled" | "unknown";
  phase: string;
  queuePosition: number;
  roughEtaMs: number;
  createdAt: string;
  updatedAt: string;
  requestHash: string;
  runtimeHint?: Record<string, unknown>;
  modelSelection?: unknown;
  artifacts?: RemoteArtifactDescriptor[];
  failure?: { code?: string; type?: string; throttleMs?: number; message?: string };
  cancellation?: { requestedAt?: string; outcome?: string };
  result?: BrowserRunResult;
  error?: string;
  errorMetadata?: { code?: string; type?: string; throttleMs?: number; message?: string };
}

export interface RemoteArtifactCapabilities {
  artifactTransfer: boolean;
  artifactProtocolVersion: number;
  maxArtifactBytes: number;
}

export interface RemoteArtifactDescriptor {
  artifactId: string;
  runId: string;
  kind: "file";
  filename: string;
  mimeType?: string;
  byteSize: number;
  sha256: string;
  validation?: SessionArtifactValidation;
  sourceUrlKind: "sandbox" | "chatgpt-file-endpoint" | "browser-download";
  transferStatus: "ready" | "streaming" | "completed" | "failed" | "skipped";
}

export type RemoteRunEvent =
  | { type: "log"; message: string }
  | { type: "artifact-ready"; runId: string; artifact: RemoteArtifactDescriptor }
  | {
      type: "artifact-progress";
      artifactId: string;
      receivedBytes?: number;
      totalBytes?: number;
      phase: "download" | "transfer" | "validate";
    }
  | { type: "result"; result: BrowserRunResult }
  | { type: "error"; message: string };

export interface SerializedAttachment extends BrowserAttachment {
  fileName: string;
  contentBase64: string;
}
