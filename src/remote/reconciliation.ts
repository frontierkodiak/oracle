import type { RemoteArtifactDescriptor } from "./types.js";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { DurableQueueStore } from "./durableQueue.js";
import {
  reopenDurableArtifactRun,
  resolveDurableArtifact,
  verifyProviderNativeCaptureArtifacts,
} from "./durableArtifacts.js";
import { TranscriptLedger, type LedgerIngestResult } from "../transcriptLedger.js";

export const RECONCILIATION_CAPABILITY_ID = "oracle.remote.reconciliation";
export const RECONCILIATION_MAX_ATTEMPTS = 3;
export interface Reconciliation {
  schemaVersion: 1;
  runId: string;
  state:
    | "paused"
    | "profile_unbound"
    | "pending"
    | "collecting"
    | "retry_wait"
    | "auth_unavailable"
    | "challenged"
    | "retry_exhausted"
    | "profile_mismatch"
    | "missing_identity"
    | "ineligible"
    | "captured_unattributed";
  conversationId?: string;
  profileId: string;
  profileBinding?: "original_dispatch" | "operator_current_profile";
  attempt: number;
  attemptLimit?: number;
  publicationFailures?: number;
  /** Persisted before admission; also the immutable ledger observation identity. */
  observationId?: string;
  captureRunId?: string;
  nextRetryAt?: string;
  lastError?: string;
  attempts?: Array<{ observationId: string; captureRunId: string; error: string }>;
  evidence?: LedgerIngestResult & {
    artifactManifestSha256: string;
    artifacts: RemoteArtifactDescriptor[];
  };
}

/** Collection uses ordinary queue admission and leases, never the original request.
 * A running child owns the browser slot. Startup makes interrupted child leases
 * unknown; their manifest is verified before retry. No clock can steal a live slot.
 */
class LedgerPublicationError extends Error {}

export class RunReconciler {
  private sweepTask?: Promise<void>;
  constructor(
    private readonly queue: DurableQueueStore,
    private readonly options: {
      enabled: boolean;
      profileId: string;
      ledgerRoot?: string;
      now?: () => number;
    },
  ) {}
  forCapture(id: string): Reconciliation | undefined {
    return this.queue
      .reconciliationRecords<Reconciliation>()
      .find(
        (record) =>
          record.captureRunId === id ||
          (record.observationId &&
            this.queue.getByIdempotencyKey(`reconciliation:${record.observationId}`)?.id === id),
      );
  }
  get(id: string): Reconciliation | undefined {
    return this.queue.reconciliation(id);
  }
  async request(id: string): Promise<Reconciliation> {
    const existing = this.get(id);
    if (existing) return existing;
    const original = this.queue.get(id);
    if (!original) throw new Error("run_not_found");
    const payload = await this.queue.request(id);
    // Reload after the asynchronous request read: another operator may have
    // created the intent while this caller yielded.
    const concurrent = this.get(id);
    if (concurrent) return concurrent;
    if (this.queue.get(id)?.state !== "unknown") throw new Error("run_not_interrupted");
    const eligible =
      payload?.browserConfig.captureOnly !== true && !this.queue.captureGrantForRun(id);
    const identity = original.runtimeHint?.conversationId;
    const conversationId =
      typeof identity === "string" && /^[a-zA-Z0-9_-]{1,128}$/.test(identity)
        ? identity
        : undefined;
    const record: Reconciliation = {
      schemaVersion: 1,
      runId: id,
      attempt: 0,
      profileId: this.queue.profileForRun(id) ?? this.options.profileId,
      ...(this.queue.profileForRun(id) ? { profileBinding: "original_dispatch" as const } : {}),
      state: !eligible
        ? "ineligible"
        : !conversationId
          ? "missing_identity"
          : original.cancellation
            ? "paused"
            : !this.queue.profileForRun(id)
              ? "profile_unbound"
              : "pending",
      ...(conversationId ? { conversationId } : {}),
    };
    this.queue.saveReconciliation(id, record);
    return record;
  }
  async resume(id: string, useCurrentProfile = false): Promise<Reconciliation> {
    await this.idle();
    const record = await this.request(id);
    if (
      !record.profileBinding &&
      useCurrentProfile &&
      record.conversationId &&
      record.state !== "ineligible"
    ) {
      record.profileId = this.options.profileId;
      record.profileBinding = "operator_current_profile";
      record.state = "paused";
      this.queue.saveReconciliation(id, record);
    }
    if (
      ["captured_unattributed", "missing_identity", "ineligible", "profile_unbound"].includes(
        record.state,
      ) ||
      !record.profileBinding
    )
      return record;
    if (record.profileId !== this.options.profileId) return record;
    if (record.state === "collecting" || record.state === "pending") return record;
    if (record.lastError === "capture_canceled" && record.captureRunId && record.observationId) {
      record.attempts = [
        ...(record.attempts ?? []),
        {
          observationId: record.observationId,
          captureRunId: record.captureRunId,
          error: "capture_canceled",
        },
      ];
      delete record.observationId;
      delete record.captureRunId;
    }
    record.attemptLimit = record.attempt + RECONCILIATION_MAX_ATTEMPTS;
    record.publicationFailures = 0;
    record.state = "pending";
    delete record.nextRetryAt;
    this.queue.saveReconciliation(id, record);
    return record;
  }
  idle(): Promise<void> {
    return this.sweepTask ?? Promise.resolve();
  }
  sweep(): Promise<void> {
    // A single in-process publication turn; the persisted child idempotency key
    // and queue lease survive a process crash at every asynchronous boundary.
    if (!this.sweepTask)
      this.sweepTask = this.sweepActive().finally(() => {
        this.sweepTask = undefined;
      });
    return this.sweepTask;
  }
  private async sweepActive(): Promise<void> {
    if (!this.options.enabled) return;
    for (const id of this.queue.reconciliationCandidates(32)) await this.request(id);
    let processed = 0;
    for (const record of this.queue.reconciliationRecords<Reconciliation>()) {
      if (
        [
          "captured_unattributed",
          "missing_identity",
          "ineligible",
          "retry_exhausted",
          "profile_mismatch",
          "profile_unbound",
          "paused",
        ].includes(record.state)
      )
        continue;
      if (record.profileId !== this.options.profileId) {
        record.state = "profile_mismatch";
        this.queue.saveReconciliation(record.runId, record);
        continue;
      }
      const now = this.options.now?.() ?? Date.now();
      if (record.nextRetryAt && Date.parse(record.nextRetryAt) > now) continue;
      if (++processed > 32) break;
      if (record.observationId) {
        // Admission may have committed before its response/link was persisted.
        const child = this.queue.getByIdempotencyKey(`reconciliation:${record.observationId}`);
        if (child) {
          record.captureRunId = child.id;
          this.queue.saveReconciliation(record.runId, record);
          if (child.cancellation) {
            record.state = "paused";
            record.lastError = "capture_canceled";
            this.queue.saveReconciliation(record.runId, record);
            continue;
          }
          if (child.state === "running" || child.state === "queued") continue;
          try {
            record.evidence = await this.collect(record);
            record.state = "captured_unattributed";
            delete record.nextRetryAt;
            delete record.lastError;
          } catch (error) {
            if (error instanceof LedgerPublicationError) {
              record.publicationFailures = (record.publicationFailures ?? 0) + 1;
              record.state = record.publicationFailures >= 3 ? "retry_exhausted" : "retry_wait";
              record.lastError = "ledger_publication_failed";
              record.nextRetryAt = new Date(
                now + 60_000 * 2 ** (record.publicationFailures - 1),
              ).toISOString();
              this.queue.saveReconciliation(record.runId, record);
              continue;
            }
            const reason = `${child.errorMetadata?.code ?? ""} ${child.error ?? ""}`.toLowerCase();
            record.lastError = reason.includes("auth")
              ? "auth_unavailable"
              : /challeng|cloudflare|bot mitigation/.test(reason)
                ? "challenged"
                : "capture_unverified";
            record.state =
              record.attempt >= (record.attemptLimit ?? RECONCILIATION_MAX_ATTEMPTS)
                ? "retry_exhausted"
                : record.lastError === "auth_unavailable"
                  ? "auth_unavailable"
                  : record.lastError === "challenged"
                    ? "challenged"
                    : "retry_wait";
            // Preserve the failed child in the receipt until the next attempt is
            // planned. Each attempt has its own immutable run and observation.
            record.nextRetryAt = new Date(
              now +
                Math.max(60_000 * 2 ** (record.attempt - 1), child.errorMetadata?.throttleMs ?? 0),
            ).toISOString();
            record.attempts = [
              ...(record.attempts ?? []),
              {
                observationId: record.observationId!,
                captureRunId: child.id,
                error: record.lastError,
              },
            ];
            delete record.observationId;
            this.queue.saveReconciliation(record.runId, record);
            continue;
          }
          this.queue.saveReconciliation(record.runId, record);
          continue;
        }
      }
      if (this.queue.admission().state !== "open") continue;
      if (!record.observationId) {
        record.attempt += 1;
        record.observationId = randomUUID();
        record.state = "collecting";
        delete record.captureRunId;
        delete record.nextRetryAt;
        this.queue.saveReconciliation(record.runId, record);
      }
      const url = `https://chatgpt.com/c/${record.conversationId!}`;
      try {
        const child = await this.queue.submit(`reconciliation:${record.observationId}`, {
          prompt: "",
          attachments: [],
          browserConfig: {
            chatgptUrl: url,
            url,
            resumeConversationUrl: url,
            captureOnly: true,
            captureProviderNative: true,
          },
          options: {},
        });
        record.captureRunId = child.id;
        this.queue.saveReconciliation(record.runId, record);
      } catch (error) {
        if (!["queue_full", "admission_draining"].includes((error as Error).message)) throw error;
        // Admission contention is not a browser attempt; retry the same intent.
      }
    }
  }
  private async collect(record: Reconciliation): Promise<NonNullable<Reconciliation["evidence"]>> {
    const runId = record.captureRunId!;
    const hint = this.queue.get(runId)?.runtimeHint;
    if (hint?.submissionAttempted === true || hint?.promptSubmitted === true)
      throw new Error("capture attempted submission");
    const run = await reopenDurableArtifactRun({ queueRoot: this.queue.root, runId });
    if (run.result.promptSubmitted !== false || run.result.conversationId !== record.conversationId)
      throw new Error("capture identity or non-submission evidence missing");
    const artifactManifestSha256 = await verifyProviderNativeCaptureArtifacts({
      queueRoot: this.queue.root,
      runId,
      descriptors: run.descriptors,
      conversationId: record.conversationId!,
      conversationUrl: `https://chatgpt.com/c/${record.conversationId}`,
    });
    const artifactPath = async (label: string) => {
      const descriptor = run.descriptors.find((item) => item.filename === label)!;
      return (
        await resolveDurableArtifact({
          queueRoot: this.queue.root,
          runId,
          artifactId: descriptor.artifactId,
        })
      ).filePath;
    };
    try {
      const ledger = await TranscriptLedger.open({
        root:
          this.options.ledgerRoot ?? path.join(path.dirname(this.queue.root), "transcript-ledger"),
      });
      try {
        const result = await ledger.ingestPair({
          provider: "chatgpt",
          profileId: this.options.profileId,
          conversationId: record.conversationId,
          observationId: record.observationId,
          canonicalUrl: `https://chatgpt.com/c/${record.conversationId}`,
          rawPath: await artifactPath("provider-native-conversation-raw"),
          evidencePath: await artifactPath("provider-native-conversation-evidence"),
          independentPath: await artifactPath("provider-native-conversation-independent"),
        });
        return { ...result, artifactManifestSha256, artifacts: run.descriptors };
      } finally {
        ledger.close();
      }
    } catch {
      throw new LedgerPublicationError("ledger publication failed");
    }
  }
}
