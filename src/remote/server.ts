import { RunReconciler, RECONCILIATION_CAPABILITY_ID } from "./reconciliation.js";
import { deriveChatgptProfileId } from "../transcriptLedger.js";
import http from "node:http";
import { createReadStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import os from "node:os";
import path from "node:path";
import net from "node:net";
import { randomBytes } from "node:crypto";
import { spawn, spawnSync } from "node:child_process";
import { mkdir, readFile, realpath, writeFile, stat } from "node:fs/promises";
import chalk from "chalk";
import type { BrowserLogger, CookieParam } from "../browser/types.js";
import { runBrowserMode } from "../browserMode.js";
import { normalizeMaxConcurrentTabs } from "../browser/tabLeaseRegistry.js";
import { loadUserConfig } from "../config.js";
import {
  CLIENT_BROWSER_CONFIG_FIELDS,
  pickClientBrowserConfig,
  sanitizeRemotePublicValue,
  type RemoteRunPayload,
  sanitizeRemoteRuntimeHint,
  summarizeRemoteBrowserLog,
} from "./types.js";
import {
  DurableQueueStore,
  CAPTURE_GRANT_CAPABILITY_ID,
  CAPTURE_GRANT_CAPABILITY_VERSION,
  DURABLE_QUEUE_CAPABILITY_ID,
  DURABLE_QUEUE_CAPABILITY_VERSION,
  MAINTENANCE_DRAIN_CAPABILITY_ID,
  MAINTENANCE_DRAIN_CAPABILITY_VERSION,
  type CaptureGrantAuthorization,
  type DurableErrorMetadata,
  type DurableRunSnapshot as StoredDurableRunSnapshot,
} from "./durableQueue.js";
import {
  persistBrowserRunArtifacts,
  resolveDurableArtifact,
  sanitizeDurableBrowserResult,
  verifyProviderNativeCaptureArtifacts,
} from "./durableArtifacts.js";
import {
  ARTIFACT_TRANSFER_FEATURE_ID,
  CAPTURE_ONLY_FEATURE_ID,
  MAX_REMOTE_ARTIFACT_BYTES,
  REMOTE_HEALTH_SCHEMA_VERSION,
} from "./types.js";
import { getOracleRuntimeIdentity } from "./runtime.js";
import { getCookies, type Cookie } from "@steipete/sweet-cookie";
import { CHATGPT_URL } from "../browser/constants.js";
import { getCliVersion } from "../version.js";
import { asOracleUserError } from "../oracle/errors.js";
import {
  buildChromeWindowModeFlags,
  connectToChrome,
  positionChromeWindowOffscreen,
  positionChromeWindowOnscreen,
} from "../browser/chromeLifecycle.js";
import {
  cleanupStaleProfileState,
  readDevToolsPort,
  verifyDevToolsReachable,
  writeChromePid,
  writeDevToolsActivePort,
} from "../browser/profileState.js";
import { normalizeChatgptUrl } from "../browser/utils.js";
import { extractStableConversationIdFromUrl } from "../browser/conversationUrl.js";
import { sanitizeArtifactFilename, sanitizeArtifactMimeType } from "../browser/artifacts.js";

export interface RemoteServerOptions {
  host?: string;
  port?: number;
  token?: string;
  logger?: (message: string) => void;
  manualLoginDefault?: boolean;
  manualLoginProfileDir?: string;
  /** Explicit serve-level override after config precedence is resolved. */
  browserHideWindow?: boolean;
  cookieSyncDefault?: boolean;
  /** Conversations that may be active at once on the shared browser profile. */
  maxConcurrentRuns?: number;
  /** Callers that may wait for a slot before the service starts refusing. */
  maxQueuedRuns?: number;
  /** Permit host-side capture-only runs; disabled unless explicitly enabled. */
  allowCaptureOnly?: boolean;
  /** Test/embedding seam; production defaults to ORACLE_HOME_DIR. */
  queueHomeDir?: string;
}

export function qualifiesForProEtaSample(
  result: Awaited<ReturnType<typeof runBrowserMode>>,
  captureOnly: boolean,
): boolean {
  const modelEvidence = result.modelSelection;
  const thinkingEvidence = result.thinkingSelection;
  return (
    !captureOnly &&
    modelEvidence?.verified === true &&
    thinkingEvidence?.verified === true &&
    thinkingEvidence.requestedLevel === "pro" &&
    result.promptSubmitted === true
  );
}

interface RemoteServerDeps {
  runBrowser?: typeof runBrowserMode;
}

interface RemoteServerInstance {
  port: number;
  token: string;
  close(): Promise<void>;
}

const ARTIFACT_PROTOCOL_VERSION = 1;
type BrowserWindowMode = "hidden" | "visible";

export function resolveServeBrowserHideWindow(
  explicitOverride: boolean | undefined,
  configuredHideWindow: boolean | undefined,
): boolean {
  return explicitOverride ?? configuredHideWindow ?? false;
}

export function resolveServeBrowserWindowMode(
  hideWindow: boolean,
  platform: NodeJS.Platform = process.platform,
): BrowserWindowMode {
  return hideWindow && platform === "darwin" ? "hidden" : "visible";
}

export function shouldApplyExistingServeBrowserWindowMode(
  explicitOverride: boolean | undefined,
  effectiveMode: BrowserWindowMode,
): boolean {
  return effectiveMode === "hidden" || explicitOverride === false;
}

class MaintenanceCaptureViolationError extends Error {
  constructor(
    readonly code: string,
    readonly terminalState: "failed" | "unknown" = "failed",
  ) {
    super(code);
    this.name = "MaintenanceCaptureViolationError";
  }
}

function artifactCapabilities(
  allowCaptureOnly: boolean,
  queue: { capacity: number; backlog: number },
) {
  const features: Array<{ id: string; version: number; limits?: Record<string, number> }> = [
    {
      id: ARTIFACT_TRANSFER_FEATURE_ID,
      version: ARTIFACT_PROTOCOL_VERSION,
      limits: { maxBytes: MAX_REMOTE_ARTIFACT_BYTES },
    },
    {
      id: DURABLE_QUEUE_CAPABILITY_ID,
      version: DURABLE_QUEUE_CAPABILITY_VERSION,
      limits: { maxQueued: queue.backlog, maxConcurrentRuns: queue.capacity },
    },
    {
      id: MAINTENANCE_DRAIN_CAPABILITY_ID,
      version: MAINTENANCE_DRAIN_CAPABILITY_VERSION,
    },
    {
      id: CAPTURE_GRANT_CAPABILITY_ID,
      version: CAPTURE_GRANT_CAPABILITY_VERSION,
    },
  ];
  if (allowCaptureOnly) features.push({ id: RECONCILIATION_CAPABILITY_ID, version: 1 });
  if (allowCaptureOnly) features.splice(1, 0, { id: CAPTURE_ONLY_FEATURE_ID, version: 1 });
  return {
    schemaVersion: REMOTE_HEALTH_SCHEMA_VERSION,
    features,
  };
}

async function findAvailablePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const srv = net.createServer();
    srv.on("error", (err) => reject(err));
    srv.listen(0, () => {
      const address = srv.address();
      if (typeof address === "object" && address?.port) {
        const port = address.port;
        srv.close(() => resolve(port));
      } else {
        srv.close(() => reject(new Error("Unable to allocate port")));
      }
    });
  });
}

export async function createRemoteServer(
  options: RemoteServerOptions = {},
  deps: RemoteServerDeps = {},
): Promise<RemoteServerInstance> {
  const serviceIdentity = await authenticatedServiceIdentity();
  const runtime = {
    ...getOracleRuntimeIdentity(),
    ...serviceIdentity,
  };
  const installSha = serviceIdentity.sourceCommit ?? null;
  const browserWindowMode = resolveServeBrowserWindowMode(options.browserHideWindow === true);
  const runBrowser = deps.runBrowser ?? runBrowserMode;
  const server = http.createServer();
  const logger = options.logger ?? console.log;
  const envToken = process.env.ORACLE_SERVE_TOKEN?.trim();
  const suppliedToken = options.token !== undefined || envToken !== undefined;
  // Env before argv-only, because a service that must be started by a supervisor
  // has nowhere to put a secret otherwise: a launchd plist or systemd unit
  // carrying `--token <secret>` writes it into a world-readable file and into
  // every `ps` listing on the host. The flag still wins when given, so nothing
  // about the interactive path changes.
  const authToken = options.token ?? envToken ?? randomBytes(16).toString("hex");
  const startedAt = Date.now();
  const verbose = process.argv.includes("--verbose") || process.env.ORACLE_SERVE_VERBOSE === "1";
  const color = process.stdout.isTTY
    ? (formatter: (msg: string) => string, msg: string) => formatter(msg)
    : (_formatter: (msg: string) => string, msg: string) => msg;
  const browserTabCap = normalizeMaxConcurrentTabs(
    (await loadUserConfig().catch(() => null))?.config.browser?.maxConcurrentTabs,
  );
  const requestedConcurrency = Math.max(1, options.maxConcurrentRuns ?? 4);
  const effectiveConcurrency = Math.min(requestedConcurrency, browserTabCap);
  if (effectiveConcurrency < requestedConcurrency) {
    logger(
      `[serve] Admitting ${effectiveConcurrency} concurrent run(s): the shared-profile tab cap (${browserTabCap}) is lower than the requested ${requestedConcurrency}.`,
    );
  }
  const durableQueue = await DurableQueueStore.open({
    homeDir: options.queueHomeDir,
    capacity: effectiveConcurrency,
    backlog: options.maxQueuedRuns ?? 8,
  });
  const hostProfileId = deriveChatgptProfileId({
    manualLoginProfileDir: options.manualLoginProfileDir,
  });
  const reconciler = new RunReconciler(durableQueue, {
    enabled: options.allowCaptureOnly === true,
    profileId: hostProfileId,
  });
  let durableWorkers = 0;
  let closing = false;
  const durableControllers = new Map<string, AbortController>();
  const durableWorkerTasks = new Set<Promise<void>>();
  const transitionIfActive = (
    id: string,
    state: Parameters<DurableQueueStore["transition"]>[1],
    phase: Parameters<DurableQueueStore["transition"]>[2],
    details: Parameters<DurableQueueStore["transition"]>[3] = {},
  ): void => {
    // Cancellation and completion are arbitrated by the SQLite transaction.
    // A worker may still be unwinding after the terminal transaction commits;
    // preserve that first terminal result instead of attempting a second one.
    const current = durableQueue.get(id)?.state;
    if (current && ["completed", "failed", "canceled", "unknown"].includes(current)) return;
    try {
      durableQueue.transition(id, state, phase, details);
    } catch (error) {
      if (
        !["completed", "failed", "canceled", "unknown"].includes(durableQueue.get(id)?.state ?? "")
      )
        throw error;
    }
  };
  const pumpDurableQueue = async (): Promise<void> => {
    if (closing) return;
    await reconciler.sweep().catch((error) => logger(`[reconciliation] ${String(error)}`));
    while (!closing && durableWorkers < effectiveConcurrency) {
      const next = durableQueue.claimNext();
      if (!next) return;
      durableWorkers += 1;
      const controller = new AbortController();
      durableControllers.set(next.id, controller);
      const worker = (async () => {
        const started = Date.now();
        const id = next.id;
        const captureGrant = durableQueue.captureGrantForRun(id);
        const reconciliation = reconciler.forCapture(id);
        try {
          if (
            reconciliation &&
            (!reconciliation.profileBinding ||
              reconciliation.profileId !== hostProfileId ||
              ["paused", "profile_mismatch", "profile_unbound"].includes(reconciliation.state))
          )
            throw new Error("reconciliation_profile_or_admission_mismatch");
          const payload = await durableQueue.request(id);
          if (!payload) throw new Error("durable request missing");
          const runDir = durableQueue.runDirectory(id);
          const materialize = async (items: any[] | undefined, folder: string) => {
            const destination = path.join(runDir, folder);
            await mkdir(destination, { recursive: true, mode: 0o700 });
            return await Promise.all(
              (items ?? []).map(async (item, index) => {
                const base = sanitizeName(item.fileName ?? `attachment-${index + 1}`);
                const ext = path.extname(base);
                const stem = ext ? base.slice(0, -ext.length) : base;
                const bytes = Buffer.from(String(item.contentBase64 ?? ""), "base64");
                for (let n = 0; n < 1000; n++) {
                  const name = n === 0 ? base : `${stem}-${n}${ext}`;
                  const target = path.join(destination, name);
                  try {
                    await writeFile(target, bytes, { flag: "wx", mode: 0o600 });
                    return {
                      path: target,
                      displayPath: item.displayPath,
                      sizeBytes: item.sizeBytes,
                    };
                  } catch (error) {
                    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
                  }
                }
                throw new Error("too many attachment name collisions");
              }),
            );
          };
          const attachments = await materialize(
            payload.attachments as any[] | undefined,
            "attachments",
          );
          const fallback = payload.fallbackSubmission as any;
          const fallbackSubmission = fallback
            ? {
                prompt: fallback.prompt,
                attachments: await materialize(
                  fallback.attachments as any[] | undefined,
                  "fallback-attachments",
                ),
              }
            : undefined;
          // Cancellation may win while request bytes and attachments are being
          // read. In that case the browser must never be invoked.
          if (durableQueue.get(id)?.state !== "running") return;
          const clientRequestedKeepBrowser = payload.browserConfig.keepBrowser === true;
          const hostConfig = {
            ...payload.browserConfig,
            inlineCookies: null,
            inlineCookiesSource: null,
            cookieSync: options.cookieSyncDefault === true,
            hideWindow: browserWindowMode === "hidden",
            ...(options.manualLoginDefault
              ? {
                  manualLogin: true,
                  manualLoginProfileDir: options.manualLoginProfileDir,
                  keepBrowser: true,
                }
              : {}),
          };
          const sessionId = captureGrant
            ? `maintenance-capture-${captureGrant.grantId}`
            : payload.options?.sessionId
              ? `${String(payload.options.sessionId)}-${id.slice(0, 8)}`
              : id;
          const automationLogger: BrowserLogger = ((message?: string) => {
            if (typeof message === "string") {
              logger(`[run ${id}] ${message}`);
              durableQueue.appendEvent(id, {
                type: "log",
                message: summarizeRemoteBrowserLog(message),
              });
            }
          }) as BrowserLogger;
          automationLogger.verbose = Boolean(payload.options?.verbose);
          durableQueue.bindProfile(id, hostProfileId);
          const result = await runBrowser({
            prompt: payload.prompt,
            attachments,
            fallbackSubmission,
            config: hostConfig as any,
            signal: controller.signal,
            log: automationLogger,
            verbose: Boolean(payload.options?.verbose),
            heartbeatIntervalMs: payload.options?.heartbeatIntervalMs as number | undefined,
            sessionId,
            followUpPrompts: payload.options?.followUpPrompts as string[] | undefined,
            closeOwnedTabOnComplete: Boolean(
              options.manualLoginDefault && !clientRequestedKeepBrowser,
            ),
            runtimeHintCb: async (hint, modelSelection) => {
              const raw = hint as unknown as Record<string, unknown>;
              const publicHint = sanitizeRemoteRuntimeHint(raw, modelSelection);
              if (reconciliation) {
                const observed = observedConversationIdFromEvidence(raw);
                if (
                  raw.submissionAttempted === true ||
                  raw.promptSubmitted === true ||
                  (observed && observed !== reconciliation.conversationId)
                )
                  throw new Error("reconciliation_capture_violation");
              }
              if (captureGrant) {
                durableQueue.recordCaptureEvidence(id, {
                  submissionAttempted: raw.submissionAttempted === true,
                  promptSubmitted: raw.promptSubmitted === true,
                });
                if (raw.submissionAttempted === true || raw.promptSubmitted === true) {
                  const code =
                    raw.promptSubmitted === true
                      ? "maintenance_capture_prompt_submitted"
                      : "maintenance_capture_submission_attempted";
                  durableQueue.recordCaptureViolation(id, code);
                  throw new MaintenanceCaptureViolationError(code, "unknown");
                }
                const observedConversationId = observedConversationIdFromEvidence(raw);
                if (
                  observedConversationId &&
                  observedConversationId !== captureGrant.conversationId
                ) {
                  const code = "maintenance_capture_wrong_conversation";
                  durableQueue.recordCaptureViolation(id, code);
                  throw new MaintenanceCaptureViolationError(code);
                }
                transitionIfActive(id, "running", "browser_attached", {
                  runtimeHint: publicHint,
                });
                return;
              }
              if (raw.submissionAttempted === true || raw.promptSubmitted === true) {
                // This write is the durable side of an irreversible send fence.
                // If cancel/shutdown won first, throw so the browser must not
                // dispatch a provider turn after recording definite cancellation.
                if (closing || controller.signal.aborted)
                  throw new Error("durable run ended before prompt dispatch");
                durableQueue.transition(id, "running", "prompt_submitted", {
                  runtimeHint: publicHint,
                });
              } else {
                transitionIfActive(id, "running", "browser_attached", {
                  runtimeHint: publicHint,
                });
              }
            },
          });
          if (
            reconciliation &&
            (result.promptSubmitted !== false ||
              (result as unknown as Record<string, unknown>).submissionAttempted === true ||
              result.conversationId !== reconciliation.conversationId)
          )
            throw new Error("reconciliation_capture_violation");
          if (captureGrant) {
            const rawResult = result as unknown as Record<string, unknown>;
            if (rawResult.submissionAttempted === true || result.promptSubmitted === true) {
              durableQueue.recordCaptureEvidence(id, {
                submissionAttempted: rawResult.submissionAttempted === true,
                promptSubmitted: result.promptSubmitted === true,
              });
              const code =
                result.promptSubmitted === true
                  ? "maintenance_capture_prompt_submitted"
                  : "maintenance_capture_submission_attempted";
              durableQueue.recordCaptureViolation(id, code);
              throw new MaintenanceCaptureViolationError(code, "unknown");
            }
            if (result.promptSubmitted !== false) {
              const code = "maintenance_capture_submission_evidence_missing";
              durableQueue.recordCaptureViolation(id, code);
              throw new MaintenanceCaptureViolationError(code);
            }
            const observedConversationId = observedConversationIdFromEvidence(rawResult);
            if (observedConversationId !== captureGrant.conversationId) {
              const code = observedConversationId
                ? "maintenance_capture_wrong_conversation"
                : "maintenance_capture_conversation_unverified";
              durableQueue.recordCaptureViolation(id, code);
              throw new MaintenanceCaptureViolationError(code);
            }
          }
          let durable: Awaited<ReturnType<typeof persistBrowserRunArtifacts>> | undefined;
          try {
            durable = await persistBrowserRunArtifacts({
              queueRoot: durableQueue.root,
              runId: id,
              result,
            });
          } catch (artifactError) {
            if (captureGrant) {
              const code = "maintenance_capture_artifact_persistence_failed";
              durableQueue.recordCaptureViolation(id, code);
              throw new MaintenanceCaptureViolationError(code);
            }
            logger(
              `[run ${id}] artifact persistence failed: ${
                artifactError instanceof Error ? artifactError.message : String(artifactError)
              }`,
            );
            const warning = {
              code: "remote-artifact-persistence-failed",
              severity: "warning" as const,
              message: "Remote artifact persistence failed on the browser host.",
            };
            transitionIfActive(id, "completed", "terminal", {
              result: sanitizeDurableBrowserResult({
                ...result,
                warnings: [...(result.warnings ?? []), warning],
              }),
              elapsedMs: Date.now() - started,
              etaQualifying: false,
            });
            return;
          }
          // Capture-only is deliberately a non-submission operation. Keep the
          // durable result honest even if a browser adapter accidentally returns
          // stale selection/submission fields.
          const durableResult =
            payload.browserConfig.captureOnly === true
              ? {
                  ...durable.result,
                  modelSelection: undefined,
                  thinkingSelection: undefined,
                }
              : durable.result;
          const modelEvidence = durableResult.modelSelection as any;
          const model = String(modelEvidence?.resolvedLabel ?? modelEvidence?.requestedModel ?? "");
          const qualifying = qualifiesForProEtaSample(
            result,
            payload.browserConfig.captureOnly === true,
          );
          let captureAudit:
            | {
                artifactManifestSha256: string;
                artifactCount: number;
                submissionAttempted: false;
                promptSubmitted: false;
              }
            | undefined;
          if (captureGrant) {
            let artifactManifestSha256: string;
            try {
              artifactManifestSha256 = await verifyProviderNativeCaptureArtifacts({
                queueRoot: durableQueue.root,
                runId: id,
                descriptors: durable.descriptors,
                conversationId: captureGrant.conversationId,
                conversationUrl: captureGrant.conversationUrl,
              });
            } catch {
              const code = "maintenance_capture_evidence_incomplete";
              durableQueue.recordCaptureViolation(id, code);
              throw new MaintenanceCaptureViolationError(code);
            }
            captureAudit = {
              artifactManifestSha256,
              artifactCount: durable.descriptors.length,
              submissionAttempted: false,
              promptSubmitted: false,
            };
            durableQueue.appendEvent(id, {
              type: "maintenance-capture-verified",
              grantId: captureGrant.grantId,
              drainId: captureGrant.drainId,
              conversationId: captureGrant.conversationId,
              submissionAttempted: false,
              promptSubmitted: false,
              artifactCount: durable.descriptors.length,
              artifactManifestSha256,
            });
          }
          transitionIfActive(id, "completed", "terminal", {
            result: { ...durableResult, artifacts: durable.descriptors },
            elapsedMs: Date.now() - started,
            model: qualifying ? "pro" : model,
            etaQualifying: qualifying,
            captureAudit,
          });
        } catch (error) {
          const phase = durableQueue.get(id)?.phase;
          const interruptedByShutdown = closing && controller.signal.aborted;
          const failure = interruptedByShutdown
            ? {
                message: "remote service shut down while the durable run was active",
                metadata: {
                  code:
                    phase === "prompt_submitted" ||
                    phase === "awaiting_response" ||
                    phase === "capturing"
                      ? "server_shutdown_after_submit"
                      : "server_shutdown_before_submit",
                  type:
                    phase === "prompt_submitted" ||
                    phase === "awaiting_response" ||
                    phase === "capturing"
                      ? "unknown"
                      : "failed",
                },
              }
            : error instanceof MaintenanceCaptureViolationError
              ? {
                  message: "Maintenance capture verification failed.",
                  metadata: { code: error.code, type: "maintenance_capture_violation" },
                }
              : formatDurableFailure(error);
          const terminalState =
            error instanceof MaintenanceCaptureViolationError
              ? error.terminalState
              : phase === "prompt_submitted" ||
                  phase === "awaiting_response" ||
                  phase === "capturing"
                ? "unknown"
                : "failed";
          transitionIfActive(id, terminalState, "terminal", {
            error: failure.message,
            errorMetadata: failure.metadata,
            elapsedMs: Date.now() - started,
            etaQualifying: false,
          });
        } finally {
          durableControllers.delete(id);
          durableWorkers -= 1;
          if (!closing) void pumpDurableQueue();
        }
      })();
      durableWorkerTasks.add(worker);
      void worker.then(
        () => durableWorkerTasks.delete(worker),
        () => durableWorkerTasks.delete(worker),
      );
    }
  };
  void pumpDurableQueue();
  const reconciliationTimer = setInterval(() => {
    if (!closing) void pumpDurableQueue();
  }, 5000);
  reconciliationTimer.unref();

  if (!process.listenerCount("unhandledRejection")) {
    process.on("unhandledRejection", (reason) => {
      logger(
        `Unhandled promise rejection in remote server: ${reason instanceof Error ? reason.message : String(reason)}`,
      );
    });
  }

  server.on("request", async (req, res) => {
    if (req.method === "GET" && req.url === "/status") {
      logger("[serve] Health check /status");
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
      return;
    }
    const bearerToken = extractBearerToken(req.headers.authorization);
    const operatorAuthorized = bearerToken === authToken;
    const captureAuthorization = operatorAuthorized
      ? undefined
      : durableQueue.authorizeCaptureGrant(bearerToken ?? "");
    const maintenancePath = req.url?.split("?")[0] ?? "";
    if (maintenancePath.startsWith("/v1/maintenance/")) {
      if (!operatorAuthorized) {
        denyAuthorization(res, captureAuthorization !== undefined);
        return;
      }
      try {
        if (req.method === "POST" && maintenancePath === "/v1/maintenance/drains") {
          const body = await readJsonObject(req, 16 * 1024);
          if (!hasExactKeys(body, ["mode"]) || body.mode !== "require-idle")
            throw new Error("invalid_maintenance_request");
          const key = req.headers["idempotency-key"];
          if (typeof key !== "string" || !key.trim())
            throw new Error("stable Idempotency-Key is required");
          const wasOpen = durableQueue.admission().state === "open";
          const drain = durableQueue.beginDrainIfIdle(key, "require-idle");
          res.writeHead(wasOpen ? 201 : 200, { "Content-Type": "application/json" });
          res.end(JSON.stringify(drain));
          return;
        }
        const drainMatch =
          /^\/v1\/maintenance\/drains\/([^/]+)(?:\/(capture-grants|release))?$/.exec(
            maintenancePath,
          );
        if (drainMatch) {
          const drainId = decodeURIComponent(drainMatch[1] ?? "");
          const action = drainMatch[2];
          if (req.method === "GET" && !action) {
            const drain = durableQueue.getDrain(drainId);
            if (!drain) throw new Error("maintenance_drain_not_found");
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify(drain));
            return;
          }
          if (req.method === "POST" && action === "capture-grants") {
            const body = await readJsonObject(req, 64 * 1024);
            if (
              !hasExactKeys(body, ["conversationUrl"]) ||
              typeof body.conversationUrl !== "string"
            )
              throw new Error("invalid_maintenance_request");
            const key = req.headers["idempotency-key"];
            if (typeof key !== "string" || !key.trim())
              throw new Error("stable Idempotency-Key is required");
            const conversation = normalizeAcceptedConversation(body.conversationUrl);
            const grant = durableQueue.issueCaptureGrant(
              drainId,
              conversation.conversationId,
              conversation.conversationUrl,
              key,
              authToken,
            );
            const { replayed, ...publicGrant } = grant;
            res.writeHead(replayed ? 200 : 201, { "Content-Type": "application/json" });
            res.end(JSON.stringify(publicGrant));
            return;
          }
          if (req.method === "POST" && action === "release") {
            const body = await readJsonObject(req, 16 * 1024);
            if (!hasExactKeys(body, [])) throw new Error("invalid_maintenance_request");
            const released = durableQueue.releaseDrain(drainId);
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify(released));
            return;
          }
        }
        const captureGrantMatch = /^\/v1\/maintenance\/capture-grants\/([^/]+)$/.exec(
          maintenancePath,
        );
        if (req.method === "GET" && captureGrantMatch) {
          const receipt = durableQueue.getCaptureGrant(
            decodeURIComponent(captureGrantMatch[1] ?? ""),
          );
          if (!receipt) throw new Error("capture_grant_not_found");
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify(receipt));
          return;
        }
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "not_found" }));
      } catch (error) {
        const failure = publicMaintenanceFailure(error);
        res.writeHead(failure.statusCode, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: failure.code }));
      }
      return;
    }
    if (req.method === "GET" && req.url === "/health") {
      if (!operatorAuthorized && captureAuthorization?.active !== true) {
        if (verbose) {
          logger(
            `[serve] Unauthorized /health attempt from ${formatSocket(req)} (missing/invalid token)`,
          );
        }
        denyAuthorization(res, captureAuthorization !== undefined);
        return;
      }
      const queueStatus = durableQueue.status();
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          ok: true,
          version: getCliVersion(),
          installSha,
          uptimeSeconds: Math.round((Date.now() - startedAt) / 1000),
          capabilities: artifactCapabilities(options.allowCaptureOnly === true, queueStatus),
          // So a caller can decide whether to send work now or later, instead of
          // discovering the answer by being queued.
          activeRuns: queueStatus.active,
          queuedRuns: queueStatus.queued,
          maxConcurrentRuns: queueStatus.capacity,
          runtime,
          process: { pid: process.pid },
          browser: { windowMode: browserWindowMode },
          queue: queueStatus,
          admission: durableQueue.admission(),
        }),
      );
      return;
    }
    const reconciliationMatch = /^\/v1\/runs\/([^/]+)\/reconciliation$/.exec(
      (req.url ?? "").split("?")[0]!,
    );
    if (reconciliationMatch) {
      if (!operatorAuthorized) {
        denyAuthorization(res, captureAuthorization !== undefined);
        return;
      }
      if (options.allowCaptureOnly !== true) {
        res.writeHead(403);
        res.end(JSON.stringify({ error: "capture_only_disabled" }));
        return;
      }
      try {
        const id = decodeURIComponent(reconciliationMatch[1]!);
        if (!durableQueue.get(id)) {
          res.writeHead(404);
          res.end();
          return;
        }
        if (req.method !== "GET" && req.method !== "POST") {
          res.writeHead(405);
          res.end();
          return;
        }
        if (req.method === "POST") {
          const body = await readRequestBody(req, 1024);
          const parsed = body.trim() ? JSON.parse(body) : {};
          const resume =
            parsed &&
            !Array.isArray(parsed) &&
            Object.keys(parsed).every((key) => key === "action" || key === "useCurrentProfile") &&
            parsed.action === "resume" &&
            (parsed.useCurrentProfile === undefined || parsed.useCurrentProfile === true);
          if (
            !resume &&
            (!parsed ||
              typeof parsed !== "object" ||
              Array.isArray(parsed) ||
              Object.keys(parsed).length !== 0)
          ) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: "reconciliation_takes_no_override" }));
            return;
          }
          if (durableQueue.get(id)?.state !== "unknown") {
            res.writeHead(409);
            res.end(JSON.stringify({ error: "run_not_interrupted" }));
            return;
          }
          if (resume) await reconciler.resume(id, parsed.useCurrentProfile === true);
          else await reconciler.request(id);
        }
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(reconciler.get(id) ?? null));
        if (req.method === "POST") void pumpDurableQueue();
        return;
      } catch {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "invalid_reconciliation_request" }));
        return;
      }
    }
    const v1Match = req.url
      ? /^\/v1\/runs\/([^/]+)(?:\/(events|cancel))?$/.exec(req.url.split("?")[0] ?? "")
      : null;
    if (req.url === "/v1/runs" || v1Match) {
      if (!operatorAuthorized && captureAuthorization?.active !== true) {
        denyAuthorization(res, captureAuthorization !== undefined);
        return;
      }
      if (req.method === "POST" && req.url === "/v1/runs") {
        const key = req.headers["idempotency-key"];
        if (typeof key !== "string" || !key.trim()) {
          res.writeHead(400);
          res.end(JSON.stringify({ error: "idempotency_key_required" }));
          return;
        }
        try {
          let snapshot: StoredDurableRunSnapshot;
          if (captureAuthorization) {
            await readRequestBody(req, 1024 * 1024);
            const payload = fixedMaintenanceCapturePayload(captureAuthorization);
            snapshot = await durableQueue.submitGrantedCapture(
              bearerToken ?? "",
              key,
              payload as any,
            );
          } else {
            const payload = JSON.parse(await readRequestBody(req)) as RemoteRunPayload;
            validateRemotePayload(payload);
            if (payload.browserConfig.captureOnly === true && options.allowCaptureOnly !== true) {
              throw new Error("capture_only_disabled");
            }
            normalizeRemotePayload(payload);
            snapshot = await durableQueue.submit(key, payload as any);
          }
          res.writeHead(202, { "Content-Type": "application/json" });
          res.end(JSON.stringify(publicDurableSnapshot(snapshot)));
          void pumpDurableQueue();
        } catch (error) {
          const failure = publicAdmissionFailure(error);
          res.writeHead(failure.statusCode, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: failure.code }));
        }
        return;
      }
      if (!v1Match) {
        res.writeHead(404);
        res.end();
        return;
      }
      const id = decodeURIComponent(v1Match[1] ?? "");
      const action = v1Match[2];
      if (captureAuthorization && captureAuthorization.runId !== id) {
        res.writeHead(403, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "forbidden" }));
        return;
      }
      if (req.method === "GET" && action === "events") {
        const url = new URL(req.url ?? "", "http://oracle.local");
        const after = Number(url.searchParams.get("after") ?? -1);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            events: publicDurableEvents(
              durableQueue.events(id, Number.isFinite(after) ? after : -1),
            ),
          }),
        );
        return;
      }
      if (req.method === "POST" && action === "cancel") {
        const snap = durableQueue.cancel(id);
        if (!snap) {
          res.writeHead(404);
          res.end();
          return;
        }
        durableControllers.get(id)?.abort();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(publicDurableSnapshot(durableQueue.get(id)!)));
        return;
      }
      if (req.method === "GET" && !action) {
        const snap = durableQueue.get(id);
        if (!snap) {
          res.writeHead(404);
          res.end();
          return;
        }
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(publicDurableSnapshot(snap)));
        return;
      }
      res.writeHead(404);
      res.end();
      return;
    }
    if (req.method === "GET" && req.url === "/v1/queue/status") {
      if (!operatorAuthorized) {
        denyAuthorization(res, captureAuthorization !== undefined);
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(durableQueue.status()));
      return;
    }
    const artifactMatch = matchArtifactRequest(req);
    if (artifactMatch) {
      if (!operatorAuthorized && captureAuthorization?.active !== true) {
        denyAuthorization(res, captureAuthorization !== undefined);
        return;
      }
      if (captureAuthorization && captureAuthorization.runId !== artifactMatch.runId) {
        res.writeHead(403, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "forbidden" }));
        return;
      }
      await serveRemoteArtifact({
        req,
        res,
        authorized: true,
        logger,
        verbose,
        runId: artifactMatch.runId,
        artifactId: artifactMatch.artifactId,
        queueRoot: durableQueue.root,
      });
      return;
    }

    if (req.method === "POST" && req.url === "/runs") {
      res.writeHead(410, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "legacy_run_endpoint_removed", endpoint: "/v1/runs" }));
      return;
    }
    if (req.method !== "POST" || req.url !== "/runs") {
      res.statusCode = 404;
      res.end();
      return;
    }
  });

  await new Promise<void>((resolve) => {
    server.listen(options.port ?? 0, options.host ?? "0.0.0.0", () => resolve());
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("Unable to determine server address.");
  }
  const reachable = formatReachableAddresses(address.address, address.port);
  const primary = reachable[0] ?? `${address.address}:${address.port}`;
  const extras = reachable.slice(1);
  const also = extras.length ? `, also [${extras.join(", ")}]` : "";
  logger(color(chalk.cyanBright.bold, `Listening at ${primary}${also}`));
  logger(
    suppliedToken
      ? "Access token supplied by caller."
      : color(chalk.yellowBright, `Access token: ${authToken}`),
  );
  logger("Leave this terminal running; press Ctrl+C to stop oracle serve.");

  return {
    port: address.port,
    token: authToken,
    async close() {
      closing = true;
      clearInterval(reconciliationTimer);
      await reconciler.idle();
      for (const controller of durableControllers.values()) controller.abort();
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
      await Promise.allSettled(durableWorkerTasks);
      durableQueue.close();
    },
  };
}

export async function serveRemote(options: RemoteServerOptions = {}): Promise<void> {
  // This must precede cookie extraction, profile setup, Chrome launch, and
  // listener creation. Unsupported runtimes must leave the host untouched.
  getOracleRuntimeIdentity();
  const configuredHideWindow = (await loadUserConfig({ includeProject: false }).catch(() => null))
    ?.config.browser?.hideWindow;
  const hideWindow = resolveServeBrowserHideWindow(options.browserHideWindow, configuredHideWindow);
  const browserWindowMode = resolveServeBrowserWindowMode(hideWindow);
  if (hideWindow && browserWindowMode === "visible") {
    console.log("browser.hideWindow is only supported on macOS; Chrome will remain visible.");
  }
  const manualProfileDir =
    options.manualLoginProfileDir ?? path.join(os.homedir(), ".oracle", "browser-profile");
  const preferManualLogin =
    options.manualLoginDefault ||
    options.cookieSyncDefault !== true ||
    process.platform === "win32" ||
    isWsl();
  let cookies: CookieParam[] | null = null;
  let opened = false;

  if (isWsl() && process.env.ORACLE_ALLOW_WSL_SERVE !== "1") {
    console.log(
      "WSL detected. For reliable browser automation, run `oracle serve` from Windows PowerShell/Command Prompt so we can use your Windows Chrome profile.",
    );
    console.log(
      "If you want to stay in WSL anyway, set ORACLE_ALLOW_WSL_SERVE=1 and ensure a Linux Chrome is installed, then rerun.",
    );
    console.log(
      "Alternatively, start Windows Chrome with --remote-debugging-port=9222 and use `--remote-chrome <windows-ip>:9222`.",
    );
    return;
  }

  if (!preferManualLogin) {
    console.log(
      "Warning: Chrome cookie copying can invalidate an active ChatGPT session when tokens rotate. Prefer the default dedicated manual-login profile when possible.",
    );
    // Warm-up: ensure this host has a ChatGPT login before accepting runs.
    const result = await loadLocalChatgptCookies(console.log, CHATGPT_URL);
    cookies = result.cookies;
    opened = result.opened;
  }

  if (!cookies || cookies.length === 0) {
    console.log("No ChatGPT cookies detected on this host.");
    if (preferManualLogin) {
      await mkdir(manualProfileDir, { recursive: true });
      console.log(
        `Cookie extraction is unavailable on this platform. Using manual-login Chrome profile at ${manualProfileDir}. Remote runs will reuse this profile; sign in once when the browser opens.`,
      );
      const existingPort = await readDevToolsPort(manualProfileDir);
      if (existingPort) {
        const reachable = await verifyDevToolsReachable({ port: existingPort });
        if (reachable.ok) {
          console.log(
            "Detected an existing automation Chrome session; will reuse it for manual login.",
          );
          if (
            shouldApplyExistingServeBrowserWindowMode(options.browserHideWindow, browserWindowMode)
          ) {
            await applyManualLoginChromeWindowMode(existingPort, browserWindowMode, console.log);
          }
        } else {
          console.log(
            `Found stale DevToolsActivePort (port ${existingPort}, ${reachable.error}); launching a fresh manual-login Chrome.`,
          );
          await cleanupStaleProfileState(manualProfileDir, console.log, {
            lockRemovalMode: "never",
          });
          void launchManualLoginChrome(
            manualProfileDir,
            CHATGPT_URL,
            console.log,
            browserWindowMode === "hidden",
          );
        }
      } else {
        void launchManualLoginChrome(
          manualProfileDir,
          CHATGPT_URL,
          console.log,
          browserWindowMode === "hidden",
        );
      }
    } else if (opened) {
      console.log(
        "Opened chatgpt.com for login. Sign in, then restart `oracle serve` to continue.",
      );
      return;
    } else {
      console.log(
        "Please open https://chatgpt.com/ in this host's browser and sign in; then rerun.",
      );
      console.log(
        "Tip: install xdg-utils (xdg-open) to enable automatic browser opening on Linux/WSL.",
      );
      return;
    }
  } else {
    console.log(
      `Detected ${cookies.length} ChatGPT cookies on this host; runs will reuse this session.`,
    );
  }

  const server = await createRemoteServer({
    ...options,
    browserHideWindow: browserWindowMode === "hidden",
    manualLoginDefault: preferManualLogin,
    manualLoginProfileDir: manualProfileDir,
  });
  await new Promise<void>((resolve) => {
    const shutdown = () => {
      console.log("Shutting down remote service...");
      server
        .close()
        .catch((error) => console.error("Failed to close remote server:", error))
        .finally(() => resolve());
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
  });
}

function matchArtifactRequest(
  req: http.IncomingMessage,
): { runId: string; artifactId: string } | null {
  if (req.method !== "GET" || !req.url) {
    return null;
  }
  let url: URL;
  try {
    url = new URL(req.url, "http://oracle.local");
  } catch {
    return null;
  }
  const match = /^\/runs\/([^/]+)\/artifacts\/([^/]+)$/.exec(url.pathname);
  if (!match) {
    return null;
  }
  try {
    return {
      runId: decodeURIComponent(match[1] ?? ""),
      artifactId: decodeURIComponent(match[2] ?? ""),
    };
  } catch {
    return null;
  }
}

async function serveRemoteArtifact(params: {
  req: http.IncomingMessage;
  res: http.ServerResponse;
  authorized: boolean;
  logger: (message: string) => void;
  verbose: boolean;
  runId: string;
  artifactId: string;
  queueRoot?: string;
}): Promise<void> {
  if (!params.authorized) {
    if (params.verbose) {
      params.logger(
        `[serve] Unauthorized artifact transfer attempt from ${formatSocket(params.req)} (missing/invalid token)`,
      );
    }
    params.res.writeHead(401, { "Content-Type": "application/json" });
    params.res.end(JSON.stringify({ error: "unauthorized" }));
    return;
  }

  if (params.queueRoot) {
    try {
      const durable = await resolveDurableArtifact({
        queueRoot: params.queueRoot,
        runId: params.runId,
        artifactId: params.artifactId,
      });
      const fileStat = await stat(durable.filePath);
      if (!fileStat.isFile() || fileStat.size <= 0) throw new Error("artifact_unavailable");
      params.res.writeHead(200, {
        "Content-Type":
          sanitizeArtifactMimeType(durable.descriptor.mimeType) ?? "application/octet-stream",
        "Content-Length": fileStat.size,
        "Content-Disposition": `attachment; filename="${sanitizeArtifactFilename(durable.descriptor.filename, "artifact.bin").replace(/"/g, "")}"`,
        "X-Oracle-Artifact-Sha256": durable.descriptor.sha256,
      });
      await pipeline(createReadStream(durable.filePath), params.res);
      return;
    } catch {
      params.res.writeHead(404, { "Content-Type": "application/json" });
      params.res.end(JSON.stringify({ error: "artifact_not_found" }));
      return;
    }
  }
}

function extractBearerToken(header: string | undefined): string | undefined {
  if (!header?.startsWith("Bearer ")) return;
  const token = header.slice("Bearer ".length);
  return token && !/\s/.test(token) ? token : undefined;
}

async function authenticatedServiceIdentity(): Promise<{
  nodeBin: string;
  oracleCli?: string;
  sourceCommit?: string;
}> {
  const nodeBin = await realpath(process.execPath).catch(() => process.execPath);
  const argvEntrypoint = process.argv[1];
  if (!argvEntrypoint) return { nodeBin };
  const oracleCli = await realpath(argvEntrypoint).catch(() => undefined);
  if (!oracleCli) return { nodeBin };
  const installRoot = path.resolve(path.dirname(oracleCli), "../..");
  let sourceCommit: string | undefined;
  try {
    const receipt = JSON.parse(
      await readFile(path.join(installRoot, "install.json"), "utf8"),
    ) as Record<string, unknown>;
    if (typeof receipt.sourceCommit === "string" && /^[a-f0-9]{40}$/.test(receipt.sourceCommit)) {
      sourceCommit = receipt.sourceCommit;
    }
  } catch {
    // Development and test invocations need not originate in an immutable install.
  }
  return { nodeBin, oracleCli, ...(sourceCommit ? { sourceCommit } : {}) };
}

function denyAuthorization(res: http.ServerResponse, recognizedScopedToken: boolean): void {
  res.writeHead(recognizedScopedToken ? 403 : 401, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: recognizedScopedToken ? "forbidden" : "unauthorized" }));
}

function hasExactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  return actual.length === wanted.length && actual.every((key, index) => key === wanted[index]);
}

async function readJsonObject(
  req: http.IncomingMessage,
  maxBytes: number,
): Promise<Record<string, unknown>> {
  const parsed = JSON.parse(await readRequestBody(req, maxBytes)) as unknown;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
    throw new Error("invalid_maintenance_request");
  return parsed as Record<string, unknown>;
}

function normalizeAcceptedConversation(raw: string): {
  conversationId: string;
  conversationUrl: string;
} {
  if (!raw.trim()) throw new Error("invalid_maintenance_request");
  const conversationUrl = normalizeChatgptUrl(raw, CHATGPT_URL);
  const parsed = new URL(conversationUrl);
  if (
    parsed.protocol !== "https:" ||
    parsed.username !== "" ||
    parsed.password !== "" ||
    (parsed.hostname !== "chatgpt.com" && parsed.hostname !== "chat.openai.com")
  )
    throw new Error("invalid_maintenance_request");
  const conversationId = extractStableConversationIdFromUrl(conversationUrl);
  if (!conversationId) throw new Error("invalid_maintenance_request");
  return { conversationId, conversationUrl };
}

function fixedMaintenanceCapturePayload(grant: CaptureGrantAuthorization): RemoteRunPayload {
  return {
    prompt: "",
    attachments: [],
    browserConfig: {
      chatgptUrl: grant.conversationUrl,
      url: grant.conversationUrl,
      resumeConversationUrl: grant.conversationUrl,
      captureOnly: true,
      captureProviderNative: true,
    },
    options: { sessionId: `maintenance-capture-${grant.grantId}` },
  };
}

function observedConversationIdFromEvidence(evidence: Record<string, unknown>): string | undefined {
  if (typeof evidence.conversationId === "string" && evidence.conversationId.trim())
    return evidence.conversationId.trim();
  for (const field of ["tabUrl", "conversationUrl"] as const) {
    const value = evidence[field];
    if (typeof value === "string") {
      const id = extractStableConversationIdFromUrl(value);
      if (id) return id;
    }
  }
  return;
}

async function readRequestBody(
  req: http.IncomingMessage,
  maxBytes = MAX_REMOTE_ARTIFACT_BYTES + 32 * 1024 * 1024,
): Promise<string> {
  const declared = Number(req.headers["content-length"] ?? 0);
  if (declared > maxBytes) throw new Error("request body too large");
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of req) {
    const buffer = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
    total += buffer.byteLength;
    if (total > maxBytes) throw new Error("request body too large");
    chunks.push(buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
}

function normalizeRemotePayload(payload: RemoteRunPayload): void {
  if (!payload || typeof payload !== "object" || !payload.browserConfig)
    throw new Error("invalid_request");
  payload.browserConfig.url = normalizeChatgptUrl(payload.browserConfig.url, CHATGPT_URL);
  payload.browserConfig = pickClientBrowserConfig(payload.browserConfig);
  if (payload.browserConfig.captureOnly === true) {
    payload.prompt = "";
    payload.attachments = [];
    payload.fallbackSubmission = undefined;
    payload.options = { ...payload.options, followUpPrompts: undefined };
    payload.browserConfig.desiredModel = undefined;
    payload.browserConfig.modelStrategy = undefined;
    payload.browserConfig.thinkingTime = undefined;
    payload.browserConfig.researchMode = undefined;
  }
}

function validateRemotePayload(payload: unknown): asserts payload is RemoteRunPayload {
  const isRecord = (value: unknown): value is Record<string, unknown> =>
    !!value && typeof value === "object" && !Array.isArray(value);
  const exact = (value: Record<string, unknown>, allowed: readonly string[]) =>
    Object.keys(value).every((key) => allowed.includes(key));
  const p = payload as Record<string, unknown>;
  if (
    !isRecord(payload) ||
    !exact(p, ["prompt", "attachments", "fallbackSubmission", "browserConfig", "options"])
  )
    throw new Error("invalid_request");
  if (typeof p.prompt !== "string" || p.prompt.length > 20_000_000 || !Array.isArray(p.attachments))
    throw new Error("invalid_request");

  const validateAttachments = (value: unknown): void => {
    if (!Array.isArray(value) || value.length > 128) throw new Error("invalid_request");
    let total = 0;
    for (const item of value) {
      if (
        !isRecord(item) ||
        !exact(item, ["fileName", "displayPath", "sizeBytes", "contentBase64"])
      )
        throw new Error("invalid_request");
      if (
        typeof item.fileName !== "string" ||
        item.fileName.length === 0 ||
        item.fileName.length > 255 ||
        typeof item.displayPath !== "string" ||
        item.displayPath.length > 2048 ||
        typeof item.contentBase64 !== "string" ||
        item.contentBase64.length > MAX_REMOTE_ARTIFACT_BYTES * 2
      )
        throw new Error("invalid_request");
      const encoded = item.contentBase64;
      if (
        encoded.length % 4 !== 0 ||
        !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(encoded)
      )
        throw new Error("invalid_request");
      const bytes = Buffer.from(encoded, "base64").byteLength;
      if (bytes > MAX_REMOTE_ARTIFACT_BYTES || total > MAX_REMOTE_ARTIFACT_BYTES - bytes)
        throw new Error("invalid_request");
      if (
        item.sizeBytes !== undefined &&
        (!Number.isSafeInteger(item.sizeBytes) ||
          (item.sizeBytes as number) < 0 ||
          item.sizeBytes !== bytes)
      )
        throw new Error("invalid_request");
      total += bytes;
    }
  };
  validateAttachments(p.attachments);
  if (p.fallbackSubmission !== undefined) {
    const f = p.fallbackSubmission;
    if (
      !isRecord(f) ||
      !exact(f, ["prompt", "attachments"]) ||
      typeof f.prompt !== "string" ||
      f.prompt.length > 20_000_000
    )
      throw new Error("invalid_request");
    validateAttachments(f.attachments);
  }
  if (!isRecord(p.browserConfig) || !exact(p.browserConfig, CLIENT_BROWSER_CONFIG_FIELDS))
    throw new Error("invalid_request");
  const configTypes: Record<string, "string" | "boolean" | "number"> = {
    chatgptUrl: "string",
    url: "string",
    desiredModel: "string",
    modelStrategy: "string",
    thinkingTime: "string",
    researchMode: "string",
    archiveConversations: "string",
    resumeConversationUrl: "string",
    captureProviderNative: "boolean",
    captureOnly: "boolean",
    timeoutMs: "number",
    inputTimeoutMs: "number",
    attachmentTimeoutMs: "number",
    assistantRecheckDelayMs: "number",
    assistantRecheckTimeoutMs: "number",
    autoReattachDelayMs: "number",
    autoReattachIntervalMs: "number",
    autoReattachTimeoutMs: "number",
    keepBrowser: "boolean",
    debug: "boolean",
  };
  for (const [key, type] of Object.entries(configTypes)) {
    if (p.browserConfig[key] !== undefined && typeof p.browserConfig[key] !== type)
      throw new Error("invalid_request");
    if (
      type === "number" &&
      p.browserConfig[key] !== undefined &&
      (!Number.isFinite(p.browserConfig[key] as number) || (p.browserConfig[key] as number) < 0)
    )
      throw new Error("invalid_request");
  }
  if (
    !isRecord(p.options) ||
    !exact(p.options, ["heartbeatIntervalMs", "verbose", "sessionId", "followUpPrompts"])
  )
    throw new Error("invalid_request");
  if (
    p.options.heartbeatIntervalMs !== undefined &&
    (typeof p.options.heartbeatIntervalMs !== "number" ||
      !Number.isFinite(p.options.heartbeatIntervalMs as number) ||
      (p.options.heartbeatIntervalMs as number) < 0)
  )
    throw new Error("invalid_request");
  if (p.options.verbose !== undefined && typeof p.options.verbose !== "boolean")
    throw new Error("invalid_request");
  if (
    p.options.sessionId !== undefined &&
    (typeof p.options.sessionId !== "string" || p.options.sessionId.length > 128)
  )
    throw new Error("invalid_request");
  if (
    p.options.followUpPrompts !== undefined &&
    (!Array.isArray(p.options.followUpPrompts) ||
      p.options.followUpPrompts.length > 32 ||
      p.options.followUpPrompts.some((x) => typeof x !== "string" || x.length > 20_000_000))
  )
    throw new Error("invalid_request");
}

function formatDurableFailure(error: unknown): {
  message: string;
  metadata: { code?: string; type?: string; message?: string; throttleMs?: number };
} {
  const oracleError = asOracleUserError(error);
  if (!oracleError) {
    const message = "Remote browser run failed on the host.";
    return { message, metadata: { type: "internal", message } };
  }
  const details = oracleError.details ?? {};
  const type = remoteFailureIdentifier(details.stage ?? details.code ?? oracleError.category);
  const captureReason = (details.details as { failure?: { reason?: unknown } } | undefined)?.failure
    ?.reason;
  const code =
    details.stage === "capture-only" &&
    (captureReason === "auth-session-unavailable" || captureReason === "challenged")
      ? captureReason
      : remoteFailureIdentifier(details.code);
  const message =
    type === "chatgpt-throttled"
      ? "ChatGPT rate limiting is active for this account; retry later."
      : type
        ? `Remote browser run failed (${type}).`
        : "Remote browser run failed on the host.";
  return {
    message,
    metadata: {
      type,
      code,
      ...(Number.isSafeInteger(details.throttleMs) && Number(details.throttleMs) >= 0
        ? { throttleMs: Number(details.throttleMs) }
        : {}),
      message,
    },
  };
}

function remoteFailureIdentifier(value: unknown): string | undefined {
  return typeof value === "string" && /^[a-zA-Z0-9._:-]{1,128}$/.test(value) ? value : undefined;
}

function publicAdmissionFailure(error: unknown): { statusCode: number; code: string } {
  const message = error instanceof Error ? error.message : String(error);
  if (message === "queue_full") return { statusCode: 503, code: "queue_full" };
  if (message === "admission_draining") return { statusCode: 503, code: "admission_draining" };
  if (message.includes("idempotency key conflicts"))
    return { statusCode: 409, code: "idempotency_conflict" };
  if (message === "capture_grant_consumed")
    return { statusCode: 409, code: "capture_grant_consumed" };
  if (message === "capture_grant_not_active")
    return { statusCode: 409, code: "capture_grant_not_active" };
  if (message === "capture_grant_invalid") return { statusCode: 401, code: "unauthorized" };
  if (message === "capture_only_disabled")
    return { statusCode: 400, code: "capture_only_disabled" };
  if (
    error instanceof SyntaxError ||
    message === "invalid_request" ||
    message === "request body too large" ||
    message === "stable Idempotency-Key is required"
  )
    return { statusCode: 400, code: "invalid_request" };
  return { statusCode: 500, code: "admission_failed" };
}

function publicMaintenanceFailure(error: unknown): { statusCode: number; code: string } {
  const message = error instanceof Error ? error.message : String(error);
  if (message === "bridge_busy") return { statusCode: 409, code: "bridge_busy" };
  if (message === "maintenance_drain_active")
    return { statusCode: 409, code: "maintenance_drain_active" };
  if (message === "maintenance_drain_not_found")
    return { statusCode: 404, code: "maintenance_drain_not_found" };
  if (message === "capture_grant_not_found")
    return { statusCode: 404, code: "capture_grant_not_found" };
  if (message === "capture_grants_pending")
    return { statusCode: 409, code: "capture_grants_pending" };
  if (message === "capture_grant_issue_conflict")
    return { statusCode: 409, code: "idempotency_conflict" };
  if (
    error instanceof SyntaxError ||
    message === "invalid_maintenance_request" ||
    message === "request body too large" ||
    message === "stable Idempotency-Key is required"
  )
    return { statusCode: 400, code: "invalid_maintenance_request" };
  return { statusCode: 500, code: "maintenance_failed" };
}

function publicFailureMetadata(
  value: DurableErrorMetadata | undefined,
): DurableErrorMetadata | undefined {
  if (!value) return undefined;
  const type = remoteFailureIdentifier(value.type);
  const code = remoteFailureIdentifier(value.code);
  const throttleMs =
    Number.isSafeInteger(value.throttleMs) && Number(value.throttleMs) >= 0
      ? value.throttleMs
      : undefined;
  const message =
    type === "chatgpt-throttled"
      ? "ChatGPT rate limiting is active for this account; retry later."
      : type
        ? `Remote browser run failed (${type}).`
        : "Remote browser run failed on the host.";
  return {
    ...(code ? { code } : {}),
    ...(type ? { type } : {}),
    ...(throttleMs !== undefined ? { throttleMs } : {}),
    message,
  };
}

function publicDurableResult(value: unknown): unknown {
  if (
    value &&
    typeof value === "object" &&
    typeof (value as any).answerText === "string" &&
    typeof (value as any).answerMarkdown === "string"
  ) {
    const result = value as Awaited<ReturnType<typeof runBrowserMode>> & {
      artifacts?: unknown[];
    };
    return {
      ...sanitizeDurableBrowserResult(result),
      ...(Array.isArray(result.artifacts)
        ? { artifacts: sanitizeRemotePublicValue(result.artifacts) }
        : {}),
    };
  }
  return sanitizeRemotePublicValue(value);
}

function publicDurableSnapshot(snapshot: StoredDurableRunSnapshot): StoredDurableRunSnapshot {
  const failure = publicFailureMetadata(snapshot.failure ?? snapshot.errorMetadata);
  return {
    ...snapshot,
    ...(snapshot.runtimeHint
      ? {
          runtimeHint: sanitizeRemoteRuntimeHint(
            snapshot.runtimeHint,
            snapshot.runtimeHint.modelSelection,
          ),
        }
      : {}),
    ...(snapshot.result !== undefined ? { result: publicDurableResult(snapshot.result) } : {}),
    ...(snapshot.error !== undefined
      ? { error: failure?.message ?? "Remote browser run failed on the host." }
      : {}),
    ...(failure ? { errorMetadata: failure, failure } : {}),
  };
}

function publicDurableEvents(
  events: Array<{ seq: number; event: unknown }>,
): Array<{ seq: number; event: unknown }> {
  return events.map(({ seq, event }) => {
    if (!event || typeof event !== "object" || Array.isArray(event))
      return { seq, event: { type: "error", message: "Remote browser event unavailable." } };
    const raw = event as Record<string, unknown>;
    if (raw.type === "log")
      return {
        seq,
        event: { type: "log", message: summarizeRemoteBrowserLog(String(raw.message ?? "")) },
      };
    if (raw.type === "error")
      return { seq, event: { type: "error", message: "Remote browser event failed." } };
    if (raw.type === "result")
      return { seq, event: { type: "result", result: publicDurableResult(raw.result) } };
    return { seq, event: sanitizeRemotePublicValue(event) };
  });
}

// Preserve the existing public import while keeping the wire allowlist shared
// by both serializer and validator.
export { pickClientBrowserConfig } from "./types.js";

function sanitizeName(raw: string): string {
  return raw.replace(/[^a-zA-Z0-9._-]/g, "_");
}

function formatSocket(req: http.IncomingMessage): string {
  const socket = req.socket;
  const host = socket.remoteAddress ?? "unknown";
  const port = socket.remotePort ?? "0";
  return `${host}:${port}`;
}

/**
 * Addresses a client could actually reach this service on.
 *
 * A loopback bind is reachable only from this machine, so listing the host's LAN
 * and tailnet addresses there is not merely noisy — it tells an operator the
 * service is exposed when it is not, which is the wrong direction for a mistake
 * about a token that grants browser automation.
 */
function formatReachableAddresses(bindAddress: string, port: number): string[] {
  if (isLoopbackAddress(bindAddress)) {
    return [`${formatHostPort(bindAddress, port)}`];
  }
  return formatAllInterfaceAddresses(bindAddress, port);
}

function isLoopbackAddress(address: string): boolean {
  const normalized = address
    .trim()
    .toLowerCase()
    .replace(/^\[|\]$/g, "");
  return normalized === "127.0.0.1" || normalized === "::1" || normalized === "localhost";
}

function formatHostPort(address: string, port: number): string {
  return address.includes(":") ? `[${address}]:${port}` : `${address}:${port}`;
}

function formatAllInterfaceAddresses(bindAddress: string, port: number): string[] {
  const ipv4: string[] = [];
  const ipv6: string[] = [];
  if (bindAddress && bindAddress !== "::" && bindAddress !== "0.0.0.0") {
    if (bindAddress.includes(":")) {
      ipv6.push(`[${bindAddress}]:${port}`);
    } else {
      ipv4.push(`${bindAddress}:${port}`);
    }
  }
  try {
    const interfaces = os.networkInterfaces();
    for (const entries of Object.values(interfaces)) {
      if (!entries) continue;
      for (const entry of entries) {
        const iface = entry as
          | { family?: string | number; address: string; internal?: boolean }
          | undefined;
        if (!iface || iface.internal) continue;
        const family =
          typeof iface.family === "string"
            ? iface.family
            : iface.family === 4
              ? "IPv4"
              : iface.family === 6
                ? "IPv6"
                : "";
        if (family === "IPv4") {
          const addr = iface.address;
          if (addr.startsWith("127.")) continue;
          if (addr.startsWith("169.254.")) continue; // APIPA/link-local
          ipv4.push(`${addr}:${port}`);
        } else if (family === "IPv6") {
          const addr = iface.address.toLowerCase();
          if (addr === "::1" || addr.startsWith("fe80:")) continue; // loopback/link-local
          ipv6.push(`[${iface.address}]:${port}`);
        }
      }
    }
  } catch {
    // network interface probing can fail in locked-down environments; ignore
  }
  // de-dup
  return Array.from(new Set([...ipv4, ...ipv6]));
}

async function loadLocalChatgptCookies(
  logger: (message: string) => void,
  targetUrl: string,
): Promise<{ cookies: CookieParam[] | null; opened: boolean }> {
  try {
    logger("Loading ChatGPT cookies from this host's Chrome profile...");
    const { cookies: rawCookies, warnings } = await getCookies({
      url: targetUrl,
      browsers: ["chrome"],
      mode: "merge",
      chromeProfile: "Default",
      timeoutMs: 5_000,
    });
    if (warnings.length) {
      logger(`Cookie warnings:\n- ${warnings.join("\n- ")}`);
    }
    const cookies = rawCookies.map(toCdpCookie).filter((c): c is CookieParam => Boolean(c));
    if (!cookies || cookies.length === 0) {
      logger("No local ChatGPT cookies found on this host. Please log in once; opening ChatGPT...");
      const opened = triggerLocalLoginPrompt(logger, targetUrl);
      return { cookies: null, opened };
    }
    logger(`Loaded ${cookies.length} local ChatGPT cookies on this host.`);
    return { cookies, opened: false };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const missingDbMatch = message.match(/Unable to locate Chrome cookie DB at (.+?)(?:\.|$)/);
    if (missingDbMatch) {
      const lookedPath = missingDbMatch[1];
      logger(
        `Chrome cookies not found at ${lookedPath}. Set --browser-cookie-path to your Chrome profile or log in manually.`,
      );
    } else {
      logger(`Unable to load local ChatGPT cookies on this host: ${message}`);
    }
    if (process.platform === "linux" && isWsl()) {
      logger(
        "WSL hint: Chrome lives under /mnt/c/Users/<you>/AppData/Local/Google/Chrome/User Data/Default; pass --browser-cookie-path to that directory if auto-detect fails.",
      );
    }
    const opened = triggerLocalLoginPrompt(logger, targetUrl);
    return { cookies: null, opened };
  }
}

function toCdpCookie(cookie: Cookie): CookieParam | null {
  if (!cookie?.name) return null;
  const out: CookieParam = {
    name: cookie.name,
    value: cookie.value,
    domain: cookie.domain,
    path: cookie.path ?? "/",
    secure: cookie.secure ?? true,
    httpOnly: cookie.httpOnly ?? false,
  };
  if (typeof cookie.expires === "number") out.expires = cookie.expires;
  if (cookie.sameSite === "Lax" || cookie.sameSite === "Strict" || cookie.sameSite === "None") {
    out.sameSite = cookie.sameSite;
  }
  return out;
}

function triggerLocalLoginPrompt(logger: (message: string) => void, url: string): boolean {
  const verbose = process.argv.includes("--verbose") || process.env.ORACLE_SERVE_VERBOSE === "1";
  const openers: Array<{ cmd: string; args?: string[] }> = [];

  if (process.platform === "darwin") {
    openers.push({ cmd: "open" });
  } else if (process.platform === "win32") {
    openers.push({ cmd: "start" });
  } else {
    if (isWsl()) {
      // Prefer wslview when available, then fall back to Windows start.exe to open in the host browser.
      openers.push({ cmd: "wslview" });
      openers.push({ cmd: "cmd.exe", args: ["/c", "start", "", url] });
    }
    openers.push({ cmd: "xdg-open" });
  }

  // Add a cross-platform, low-friction fallback when nothing above is available.
  openers.push({ cmd: "sensible-browser" });

  try {
    // Fire and forget; user completes login in the opened browser window.
    if (verbose) {
      logger(`[serve] Login opener candidates: ${openers.map((o) => o.cmd).join(", ")}`);
    }
    const candidate = openers.find((opener) => canSpawn(opener.cmd));
    if (candidate) {
      const child = spawn(candidate.cmd, candidate.args ?? [url], {
        stdio: "ignore",
        detached: true,
      });
      child.unref();
      child.once("error", (error) => {
        if (verbose) {
          logger(
            `[serve] Opener ${candidate.cmd} failed: ${error instanceof Error ? error.message : String(error)}`,
          );
        }
        logger(`Please open ${url} in this host's browser and sign in; then rerun.`);
      });
      logger(
        `Opened ${url} locally via ${candidate.cmd}. Please sign in; subsequent runs will reuse the session.`,
      );
      if (verbose && candidate.args) {
        logger(`[serve] Opener args: ${candidate.args.join(" ")}`);
      }
      return true;
    }
    if (verbose) {
      logger("[serve] No available opener found; prompting manual login.");
    }
    return false;
  } catch {
    return false;
  }
}

function isWsl(): boolean {
  if (process.platform !== "linux") return false;
  return Boolean(process.env.WSL_DISTRO_NAME || os.release().toLowerCase().includes("microsoft"));
}

function canSpawn(cmd: string): boolean {
  if (!cmd) return false;
  try {
    if (process.platform === "win32") {
      // `where` returns non-zero when the command is not found.
      const result = spawnSync("where", [cmd], { stdio: "ignore" });
      return result.status === 0;
    }
    // `command -v` is a shell builtin; run through sh. Fallback to `which`.
    const shResult = spawnSync("sh", ["-c", `command -v ${cmd}`], { stdio: "ignore" });
    if (shResult.status === 0) return true;
    const whichResult = spawnSync("which", [cmd], { stdio: "ignore" });
    return whichResult.status === 0;
  } catch {
    return false;
  }
}

async function applyManualLoginChromeWindowMode(
  port: number,
  mode: BrowserWindowMode,
  logger: (msg: string) => void,
): Promise<void> {
  const browserLogger = logger as BrowserLogger;
  try {
    const client = await connectToChrome(port, browserLogger);
    try {
      if (mode === "hidden") {
        await positionChromeWindowOffscreen(client, browserLogger);
      } else {
        await positionChromeWindowOnscreen(client, browserLogger);
      }
    } finally {
      await client.close().catch(() => undefined);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logger(`Unable to apply ${mode} mode to the existing manual-login Chrome: ${message}`);
  }
}

export function buildManualLoginChromeFlags(
  profileDir: string,
  debugPort: number,
  hideWindow: boolean,
  platform: NodeJS.Platform = process.platform,
): string[] {
  return [
    "--no-first-run",
    "--no-default-browser-check",
    `--user-data-dir=${profileDir}`,
    "--remote-allow-origins=*",
    `--remote-debugging-port=${debugPort}`,
    ...buildChromeWindowModeFlags(false, hideWindow, platform),
  ];
}

async function launchManualLoginChrome(
  profileDir: string,
  url: string,
  logger: (msg: string) => void,
  hideWindow = false,
): Promise<void> {
  const timeoutMs = 7000;
  let finished = false;
  const timeout = setTimeout(() => {
    if (!finished) {
      logger(
        `Timed out launching Chrome for manual login. Launch Chrome manually with --user-data-dir=${profileDir} and log in to ${url}.`,
      );
    }
  }, timeoutMs);

  try {
    const chromeLauncher = await import("chrome-launcher");
    const { launch } = chromeLauncher;
    const debugPort = await findAvailablePort();
    logger(`Planned manual-login Chrome DevTools port: ${debugPort}`);
    const chrome = await launch({
      // Expose DevTools so later runs can attach instead of spawning a second Chrome.
      // Use a per-serve free port so the login window stays stable for all runs.
      port: debugPort,
      userDataDir: profileDir,
      startingUrl: url,
      chromeFlags: buildManualLoginChromeFlags(profileDir, debugPort, hideWindow),
    });

    const chosenPort = chrome?.port ?? debugPort ?? null;
    if (chosenPort) {
      // Persist DevToolsActivePort eagerly so future runs can attach/reuse this Chrome.
      await writeDevToolsActivePort(profileDir, chosenPort);
      if (chrome?.pid) {
        await writeChromePid(profileDir, chrome.pid);
      }
      logger(`Manual-login Chrome DevTools port: ${chosenPort}`);
      logger(`If needed, DevTools JSON at http://127.0.0.1:${chosenPort}/json/version`);
    } else {
      logger(
        "Warning: unable to determine manual-login Chrome DevTools port. Remote runs may fail to attach.",
      );
    }

    finished = true;
    clearTimeout(timeout);
    const portInfo = chosenPort ? ` (DevTools port ${chosenPort})` : "";
    logger(
      `Opened Chrome with manual-login profile at ${profileDir}${portInfo}. Complete login, then rerun remote sessions.`,
    );
  } catch (error) {
    finished = true;
    clearTimeout(timeout);
    const message = error instanceof Error ? error.message : String(error);
    logger(
      `Unable to open Chrome for manual login (${message}). Launch Chrome manually with --user-data-dir=${profileDir} and log in to ${url}.`,
    );
  }
}
