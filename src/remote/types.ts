import type { BrowserSessionConfig } from "../sessionStore.js";
import type { BrowserRunResult } from "../browserMode.js";
import type { BrowserAttachment } from "../browser/types.js";
import type { SessionArtifactValidation } from "../sessionManager.js";

export const MAX_REMOTE_ARTIFACT_BYTES = 512 * 1024 * 1024;
export const REMOTE_HEALTH_SCHEMA_VERSION = 1;
export const ARTIFACT_TRANSFER_FEATURE_ID = "oracle.remote.artifact-transfer";
export const CAPTURE_ONLY_FEATURE_ID = "oracle.browser.capture-only";
export const DURABLE_QUEUE_FEATURE_ID = "oracle.remote.durable-queue";

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
