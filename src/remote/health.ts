import http from "node:http";
import net from "node:net";
import { parseHostPort } from "../bridge/connection.js";
import {
  assertSupportedNodeVersion,
  ORACLE_MIN_NODE_MAJOR,
  type OracleRuntimeIdentity,
} from "./runtime.js";
import {
  ARTIFACT_TRANSFER_FEATURE_ID,
  DURABLE_QUEUE_FEATURE_ID,
  MAX_REMOTE_ARTIFACT_BYTES,
  REMOTE_HEALTH_SCHEMA_VERSION,
  type RemoteArtifactCapabilities,
  type RemoteCapabilityFeature,
  type RemoteCapabilityManifest,
} from "./types.js";

export interface RemoteHealthResult {
  ok: boolean;
  statusCode?: number;
  error?: string;
  version?: string;
  installSha?: string | null;
  uptimeSeconds?: number;
  runtime?: OracleRuntimeIdentity;
  manifest?: RemoteCapabilityManifest;
  capabilities?: RemoteArtifactCapabilities;
  browser?: RemoteBrowserHealth;
}

export interface RemoteBrowserHealth {
  windowMode: "hidden" | "visible";
}

export async function checkTcpConnection(
  host: string,
  timeoutMs = 2000,
): Promise<{ ok: boolean; error?: string }> {
  const { hostname, port } = parseHostPort(host);
  return await new Promise((resolve) => {
    const socket = net.createConnection({ host: hostname, port });
    const onError = (err: Error) => {
      cleanup();
      resolve({ ok: false, error: err.message });
    };
    const onConnect = () => {
      cleanup();
      resolve({ ok: true });
    };
    const onTimeout = () => {
      cleanup();
      resolve({ ok: false, error: `timeout after ${timeoutMs}ms` });
    };
    const cleanup = () => {
      socket.removeAllListeners();
      socket.end();
      socket.destroy();
      socket.unref();
    };
    socket.setTimeout(timeoutMs);
    socket.once("error", onError);
    socket.once("connect", onConnect);
    socket.once("timeout", onTimeout);
  });
}

export async function checkRemoteHealth({
  host,
  token,
  timeoutMs = 5000,
}: {
  host: string;
  token?: string;
  timeoutMs?: number;
}): Promise<RemoteHealthResult> {
  const { hostname, port } = parseHostPort(host);
  const headers: Record<string, string> = { accept: "application/json" };
  if (token) {
    headers.authorization = `Bearer ${token}`;
  }
  try {
    const response = await requestJson({
      hostname,
      port,
      path: "/health",
      headers,
      timeoutMs,
    });
    if (response.statusCode === 200 && typeof response.json === "object" && response.json) {
      const ok = (response.json as { ok?: unknown }).ok === true;
      const version = (response.json as { version?: unknown }).version;
      const uptimeSeconds = (response.json as { uptimeSeconds?: unknown }).uptimeSeconds;
      const parsed = parseHealthEnvelope(response.json);
      if (!parsed)
        return {
          ok: false,
          statusCode: response.statusCode,
          error: "malformed /health handshake (upgrade oracle on the host and retry)",
        };
      return {
        ok: ok && parsed.ok,
        statusCode: response.statusCode,
        version: typeof version === "string" ? version : undefined,
        installSha: parsed.installSha,
        uptimeSeconds: typeof uptimeSeconds === "number" ? uptimeSeconds : undefined,
        runtime: parsed.runtime,
        manifest: parsed.manifest,
        capabilities: parsed.artifact,
        browser: parsed.browser,
      };
    }
    if (response.statusCode === 404) {
      return {
        ok: false,
        statusCode: response.statusCode,
        error: "remote host does not expose /health (upgrade oracle on the host and retry)",
      };
    }
    const error =
      extractErrorMessage(response.json, response.bodyText) ?? `HTTP ${response.statusCode}`;
    return { ok: false, statusCode: response.statusCode, error };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error) };
  }
}

export function parseHealthEnvelope(value: unknown):
  | {
      ok: boolean;
      runtime: OracleRuntimeIdentity;
      manifest: RemoteCapabilityManifest;
      artifact?: RemoteArtifactCapabilities;
      installSha?: string | null;
      browser?: RemoteBrowserHealth;
    }
  | undefined {
  if (!value || typeof value !== "object") return undefined;
  const raw = value as Record<string, unknown>;
  const runtime = raw.runtime;
  if (!runtime || typeof runtime !== "object") return undefined;
  const rt = runtime as Record<string, unknown>;
  if (
    raw.ok !== true ||
    typeof raw.version !== "string" ||
    raw.version.trim().length === 0 ||
    rt.name !== "node" ||
    typeof rt.version !== "string" ||
    typeof rt.major !== "number" ||
    !Number.isSafeInteger(rt.major) ||
    rt.minimumMajor !== ORACLE_MIN_NODE_MAJOR
  )
    return undefined;
  try {
    assertSupportedNodeVersion(rt.version);
  } catch {
    return undefined;
  }
  if (rt.major !== Number(rt.version.split(".")[0])) return undefined;
  const installSha = raw.installSha;
  if (
    installSha !== undefined &&
    installSha !== null &&
    (typeof installSha !== "string" || !/^[0-9a-f]{40}$/i.test(installSha))
  )
    return undefined;
  const rawBrowser = raw.browser;
  let browser: RemoteBrowserHealth | undefined;
  if (rawBrowser !== undefined) {
    if (!rawBrowser || typeof rawBrowser !== "object" || Array.isArray(rawBrowser))
      return undefined;
    const windowMode = (rawBrowser as Record<string, unknown>).windowMode;
    if (windowMode !== "hidden" && windowMode !== "visible") return undefined;
    browser = { windowMode };
  }
  const caps = raw.capabilities;
  if (!caps || typeof caps !== "object") return undefined;
  const c = caps as Record<string, unknown>;
  if (c.schemaVersion !== REMOTE_HEALTH_SCHEMA_VERSION || !Array.isArray(c.features))
    return undefined;
  const features: RemoteCapabilityFeature[] = [];
  const ids = new Set<string>();
  for (const item of c.features) {
    if (!item || typeof item !== "object") return undefined;
    const f = item as Record<string, unknown>;
    if (
      typeof f.id !== "string" ||
      f.id.trim().length === 0 ||
      f.id !== f.id.trim() ||
      !f.id.includes(".") ||
      typeof f.version !== "number" ||
      !Number.isSafeInteger(f.version) ||
      f.version <= 0 ||
      ids.has(f.id)
    )
      return undefined;
    ids.add(f.id);
    if (
      f.limits !== undefined &&
      (!f.limits || typeof f.limits !== "object" || Array.isArray(f.limits))
    )
      return undefined;
    const limits = f.limits as Record<string, unknown> | undefined;
    if (
      limits?.maxBytes !== undefined &&
      (typeof limits.maxBytes !== "number" ||
        !Number.isSafeInteger(limits.maxBytes) ||
        limits.maxBytes <= 0)
    )
      return undefined;
    if (f.id === DURABLE_QUEUE_FEATURE_ID) {
      if (
        !limits ||
        !Number.isSafeInteger(limits.maxQueued) ||
        Number(limits.maxQueued) < 0 ||
        !Number.isSafeInteger(limits.maxConcurrentRuns) ||
        Number(limits.maxConcurrentRuns) < 1
      )
        return undefined;
    }
    features.push({ id: f.id, version: f.version, ...(limits ? { limits: { ...limits } } : {}) });
  }
  const artifactFeature = features.find(
    (f) => f.id === ARTIFACT_TRANSFER_FEATURE_ID && f.version === 1,
  );
  const artifactMaxBytes = artifactFeature?.limits?.maxBytes;
  const artifact =
    typeof artifactMaxBytes === "number" && artifactMaxBytes > 0
      ? {
          artifactTransfer: true,
          artifactProtocolVersion: 1,
          maxArtifactBytes: Math.min(artifactMaxBytes, MAX_REMOTE_ARTIFACT_BYTES),
        }
      : undefined;
  return {
    ok: raw.ok === true,
    runtime: rt as unknown as OracleRuntimeIdentity,
    manifest: { schemaVersion: REMOTE_HEALTH_SCHEMA_VERSION, features },
    artifact,
    ...(installSha !== undefined ? { installSha: installSha as string | null } : {}),
    ...(browser ? { browser } : {}),
  };
}

function extractErrorMessage(json: unknown, bodyText: string): string | null {
  if (json && typeof json === "object") {
    const err = (json as { error?: unknown }).error;
    if (typeof err === "string" && err.trim().length > 0) {
      return err.trim();
    }
  }
  const trimmed = bodyText.trim();
  return trimmed.length ? trimmed : null;
}

async function requestJson({
  hostname,
  port,
  path,
  headers,
  timeoutMs,
}: {
  hostname: string;
  port: number;
  path: string;
  headers: Record<string, string>;
  timeoutMs: number;
}): Promise<{ statusCode: number; json: unknown; bodyText: string }> {
  return await new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname,
        port,
        path,
        method: "GET",
        headers,
      },
      (res) => {
        res.setEncoding("utf8");
        let body = "";
        res.on("data", (chunk: string) => {
          body += chunk;
        });
        res.on("end", () => {
          const statusCode = res.statusCode ?? 0;
          let json: unknown = null;
          try {
            json = body.length ? JSON.parse(body) : null;
          } catch {
            json = null;
          }
          resolve({ statusCode, json, bodyText: body });
        });
      },
    );
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`timeout after ${timeoutMs}ms`));
    });
    req.on("error", reject);
    req.end();
  });
}
