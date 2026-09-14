import { createHash, randomUUID } from "node:crypto";
import path from "node:path";
import { mkdir, writeFile } from "node:fs/promises";
import type { SessionArtifact } from "../sessionStore.js";
import { resolveSessionArtifactsDir, resolveUniqueArtifactPath } from "./artifacts.js";
import type { BrowserLogger, ChromeClient } from "./types.js";

export type ProviderNativeFailureReason =
  | "no-conversation-id"
  | "invalid-conversation-id"
  | "wrong-origin"
  | "auth-session-unavailable"
  | "challenged"
  | "http-error"
  | "empty-document"
  | "invalid-document"
  | "document-too-large"
  | "evaluate-failed"
  | "timeout"
  | "digest-unavailable"
  | "write-failed";

export interface ProviderNativeCaptureFailure {
  reason: ProviderNativeFailureReason;
  httpStatus?: number;
}

export interface ProviderNativeTurnDigest {
  messageId: string | null;
  nodeId: string;
  role: string;
  contentType: string;
  /** Unsupported/non-text content has no digest, rather than a guessed serialization. */
  sha256: string | null;
  bytes: number | null;
}

interface ProviderEvidence {
  documentSha256: string;
  documentBytes: number;
  perTurn: ProviderNativeTurnDigest[];
  fetchedAt: string;
}

export interface ProviderNativeCapture {
  conversationId: string;
  rawText: string;
  rawSha256: string;
  rawBytes: number;
  evidence: ProviderEvidence | null;
  evidenceFailure?: ProviderNativeCaptureFailure;
  documentHashesMatch: boolean | null;
}

export type ProviderNativeCaptureOutcome =
  | { status: "captured"; capture: ProviderNativeCapture }
  | { status: "unavailable"; failure: ProviderNativeCaptureFailure };

export type AnswerFidelity = "matched" | "divergent" | "unknown";

export interface ProviderNativeCaptureSummary {
  status: "captured" | "unavailable";
  answerFidelity: AnswerFidelity;
  answerMatch?: "exact" | "trimmed";
  answerMessageId?: string;
  conversationId?: string;
  rawSha256?: string;
  rawBytes?: number;
  turnCount?: number;
  documentHashesMatch?: boolean | null;
  materializedToDisk?: boolean;
  failure?: ProviderNativeCaptureFailure;
  evidenceFailure?: ProviderNativeCaptureFailure;
  capturedAt?: string;
}

const MAX_DOCUMENT_BYTES = 8 * 1024 * 1024;
const CHUNK_CHARS = 250_000;
const CAPTURE_TIMEOUT_MS = 30_000;

// These expressions run inside the existing authenticated page/connection. Auth
// values and arbitrary exception messages must never cross back into diagnostics.
function fetchSource(conversationId: string, deadline: number): string {
  return `
    const deadline = ${deadline};
    const fetchDocument = async () => {
      if (!['https://chatgpt.com', 'https://chat.openai.com'].includes(location.origin)) {
        return {ok:false, reason:'wrong-origin'};
      }
      const signal = AbortSignal.timeout(Math.max(1, deadline - Date.now()));
      const auth = await fetch('/api/auth/session', {credentials:'include', signal, redirect:'error'});
      if (!auth.ok) return {ok:false, reason:'auth-session-unavailable', httpStatus:auth.status};
      const session = await auth.json();
      if (typeof session?.accessToken !== 'string' || !session.accessToken) {
        return {ok:false, reason:'auth-session-unavailable'};
      }
      const response = await fetch('/backend-api/conversation/' + ${JSON.stringify(conversationId)}, {
        credentials:'include', signal, redirect:'error',
        headers:{Authorization:'Bearer ' + session.accessToken, Accept:'application/json'}
      });
      if (response.status === 403 || (response.headers.get('content-type') || '').includes('text/html')) {
        return {ok:false, reason:'challenged', httpStatus:response.status};
      }
      if (!response.ok) return {ok:false, reason:'http-error', httpStatus:response.status};
      if (!response.body) return {ok:false, reason:'empty-document'};
      const reader = response.body.getReader();
      const decoder = new TextDecoder('utf-8', {fatal:true});
      let text = '', bytes = 0;
      try {
        for (;;) {
          const {done, value} = await reader.read();
          if (done) break;
          bytes += value.byteLength;
          if (bytes > ${MAX_DOCUMENT_BYTES}) return {ok:false, reason:'document-too-large'};
          text += decoder.decode(value, {stream:true});
        }
        text += decoder.decode();
      } finally { await reader.cancel().catch(() => {}); }
      if (!text) return {ok:false, reason:'empty-document'};
      try {
        const doc = JSON.parse(text);
        if (!doc || !doc.mapping || typeof doc.mapping !== 'object' || Array.isArray(doc.mapping) ||
            (doc.conversation_id !== undefined && doc.conversation_id !== ${JSON.stringify(conversationId)})) {
          return {ok:false, reason:'invalid-document'};
        }
      } catch { return {ok:false, reason:'invalid-document'}; }
      return {ok:true, text};
    };
  `;
}

function digestSource(source: string): string {
  return `
    if (!globalThis.crypto?.subtle) return {ok:false, reason:'digest-unavailable'};
    const result = await (${source});
    if (!result.ok) return result;
    const doc = JSON.parse(result.text);
    const mapping = doc?.mapping;
    if (!mapping || typeof mapping !== 'object' || Array.isArray(mapping) ||
        typeof doc.current_node !== 'string' || !Object.hasOwn(mapping, doc.current_node)) {
      return {ok:false, reason:'invalid-document'};
    }
    const chain = [], seen = new Set();
    let id = doc.current_node;
    while (id !== null && id !== undefined) {
      if (typeof id !== 'string' || seen.has(id) || !Object.hasOwn(mapping, id) || !mapping[id]) {
        return {ok:false, reason:'invalid-document'};
      }
      seen.add(id); chain.push(id); id = mapping[id].parent;
    }
    const hash = async (text) => {
      const bytes = new TextEncoder().encode(text);
      const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
      return {sha256:Array.from(digest, b => b.toString(16).padStart(2, '0')).join(''), bytes:bytes.length};
    };
    const perTurn = [];
    for (const nodeId of chain.reverse()) {
      const message = mapping[nodeId].message;
      if (!message || message.author?.role === 'system') continue;
      const content = message.content;
      const contentType = typeof content?.content_type === 'string' ? content.content_type : 'unknown';
      let body = null;
      if (contentType === 'text' || contentType === 'multimodal_text') {
        if (Array.isArray(content.parts) && content.parts.every(p => typeof p === 'string')) {
          body = content.parts.join('\\n\\n');
        }
      } else if (contentType === 'code' || contentType === 'execution_output') {
        if (typeof content.text === 'string') body = content.text;
      } else if (contentType === 'reasoning_recap') {
        if (typeof content.content === 'string') body = content.content;
      } else if (contentType === 'thoughts') {
        if (Array.isArray(content.thoughts) && content.thoughts.every(t => typeof t?.content === 'string')) {
          body = content.thoughts.map(t => t.content).join('\\n\\n');
        }
      }
      const digest = body === null ? {sha256:null, bytes:null} : await hash(body);
      perTurn.push({nodeId, messageId:typeof message.id === 'string' ? message.id : null,
        role:typeof message.author?.role === 'string' ? message.author.role : 'unknown',
        contentType, ...digest});
    }
    const documentDigest = await hash(result.text);
    return {ok:true, documentSha256:documentDigest.sha256, documentBytes:documentDigest.bytes,
      perTurn, fetchedAt:new Date().toISOString()};
  `;
}

export function buildNormalizeAndDigestExpressionForTest(rawText: string): string {
  return `(async () => { ${digestSource(`{ok:true,text:${JSON.stringify(rawText)}}`)} })()`;
}

interface PageFailure extends ProviderNativeCaptureFailure {
  ok: false;
}

async function evaluate<T>(
  Runtime: ChromeClient["Runtime"],
  expression: string,
  deadline: number,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error("timeout");
    const response = await Promise.race([
      Runtime.evaluate({ expression, awaitPromise: true, returnByValue: true }),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("timeout")), remaining);
      }),
    ]);
    if (response.exceptionDetails || response.result?.value === undefined)
      throw new Error("evaluate-failed");
    return response.result.value as T;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export async function captureProviderNativeConversation(params: {
  Runtime: ChromeClient["Runtime"];
  conversationId: string | null | undefined;
}): Promise<ProviderNativeCaptureOutcome> {
  const { Runtime } = params;
  const conversationId = params.conversationId?.trim();
  if (!conversationId) return { status: "unavailable", failure: { reason: "no-conversation-id" } };
  if (!/^[a-zA-Z0-9-]{1,128}$/.test(conversationId)) {
    return { status: "unavailable", failure: { reason: "invalid-conversation-id" } };
  }
  const deadline = Date.now() + CAPTURE_TIMEOUT_MS;
  const key = JSON.stringify(`__oracleCapture_${randomUUID()}`);
  const source = fetchSource(conversationId, deadline);
  const safeExpression = (body: string) =>
    `(async () => { try { ${body} } catch { return {ok:false, reason:'evaluate-failed'}; } })()`;
  try {
    const head = await evaluate<{ ok: true; length: number } | PageFailure>(
      Runtime,
      safeExpression(`
      ${source}
      const result = await fetchDocument();
      if (!result.ok) return result;
      if (Date.now() >= deadline) return {ok:false, reason:'timeout'};
      globalThis[${key}] = result.text;
      setTimeout(() => { delete globalThis[${key}]; }, Math.max(1, deadline - Date.now()));
      return {ok:true, length:result.text.length};
    `),
      deadline,
    );
    if (!head.ok)
      return {
        status: "unavailable",
        failure: { reason: head.reason, httpStatus: head.httpStatus },
      };
    if (
      !Number.isSafeInteger(head.length) ||
      head.length <= 0 ||
      head.length > MAX_DOCUMENT_BYTES
    ) {
      return { status: "unavailable", failure: { reason: "document-too-large" } };
    }
    let rawText = "";
    while (rawText.length < head.length) {
      const chunk = await evaluate<unknown>(
        Runtime,
        `globalThis[${key}]?.slice(${rawText.length}, ${rawText.length + CHUNK_CHARS})`,
        deadline,
      );
      if (typeof chunk !== "string" || !chunk || chunk.length > CHUNK_CHARS)
        throw new Error("evaluate-failed");
      rawText += chunk;
    }
    if (rawText.length !== head.length) throw new Error("evaluate-failed");
    const capture: ProviderNativeCapture = {
      conversationId,
      rawText,
      rawSha256: createHash("sha256").update(rawText).digest("hex"),
      rawBytes: Buffer.byteLength(rawText),
      evidence: null,
      documentHashesMatch: null,
    };
    try {
      // A second request hashes the active branch in the page; its body never leaves Chrome.
      const evidence = await evaluate<({ ok: true } & ProviderEvidence) | PageFailure>(
        Runtime,
        safeExpression(`${source} ${digestSource("fetchDocument()")}`),
        deadline,
      );
      if (evidence.ok) {
        capture.evidence = evidence;
        capture.documentHashesMatch = evidence.documentSha256 === capture.rawSha256;
      } else capture.evidenceFailure = { reason: evidence.reason, httpStatus: evidence.httpStatus };
    } catch {
      capture.evidenceFailure = { reason: Date.now() >= deadline ? "timeout" : "evaluate-failed" };
    }
    return { status: "captured", capture };
  } catch {
    return {
      status: "unavailable",
      failure: { reason: Date.now() >= deadline ? "timeout" : "evaluate-failed" },
    };
  } finally {
    // Do not wait on a disconnected renderer merely to release optional evidence.
    void Runtime.evaluate({ expression: `delete globalThis[${key}]`, returnByValue: true }).catch(
      () => {},
    );
  }
}

function compareAnswer(
  answer: string | undefined,
  messageId: string | undefined,
  turns?: ProviderNativeTurnDigest[],
): { answerFidelity: AnswerFidelity; answerMatch?: "exact" | "trimmed" } {
  if (!answer || !messageId) return { answerFidelity: "unknown" };
  const candidates = turns?.filter((t) => t.messageId === messageId && t.role === "assistant");
  const turn = candidates?.length === 1 ? candidates[0] : undefined;
  if (!turn?.sha256 || !["text", "code", "multimodal_text"].includes(turn.contentType)) {
    return { answerFidelity: "unknown" };
  }
  for (const [kind, text] of [
    ["exact", answer],
    ["trimmed", answer.trim()],
  ] as const) {
    if (createHash("sha256").update(text).digest("hex") === turn.sha256) {
      return { answerFidelity: "matched", answerMatch: kind };
    }
  }
  return { answerFidelity: "divergent" };
}

/** Best-effort evidence only: copy-button/DOM answer capture remains authoritative for output. */
export async function finalizeProviderNativeCapture(params: {
  Runtime: ChromeClient["Runtime"];
  conversationId: string | null | undefined;
  conversationUrl?: string | null;
  sessionId?: string;
  answerMarkdown?: string;
  answerMessageId?: string;
  logger?: BrowserLogger;
}): Promise<{ summary: ProviderNativeCaptureSummary; artifacts: SessionArtifact[] }> {
  const artifacts: SessionArtifact[] = [];
  const summary: ProviderNativeCaptureSummary = {
    status: "unavailable",
    answerFidelity: "unknown",
  };
  // Logging is optional too, and must not turn a successful answer into a failure.
  const log = (message: string) => {
    try {
      params.logger?.(message);
    } catch {}
  };
  try {
    const outcome = await captureProviderNativeConversation(params);
    if (outcome.status === "unavailable") {
      summary.failure = outcome.failure;
      log(
        `[capture] Provider-native capture unavailable (${outcome.failure.reason}); the answer is unaffected.`,
      );
      return { summary, artifacts };
    }
    const capture = outcome.capture;
    Object.assign(summary, {
      status: "captured",
      ...compareAnswer(params.answerMarkdown, params.answerMessageId, capture.evidence?.perTurn),
      answerMessageId: params.answerMessageId,
      conversationId: capture.conversationId,
      rawSha256: capture.rawSha256,
      rawBytes: capture.rawBytes,
      turnCount: capture.evidence?.perTurn.length,
      documentHashesMatch: capture.documentHashesMatch,
      evidenceFailure: capture.evidenceFailure,
      capturedAt: new Date().toISOString(),
      materializedToDisk: false,
    });
    if (!params.sessionId) return { summary, artifacts };
    const dir = resolveSessionArtifactsDir(params.sessionId);
    await mkdir(dir, { recursive: true, mode: 0o700 });
    const rawPath = await resolveUniqueArtifactPath(
      path.join(dir, `conversation-${capture.conversationId}-raw.json`),
    );
    await writeFile(rawPath, capture.rawText, { encoding: "utf8", mode: 0o600, flag: "wx" });
    summary.materializedToDisk = true;
    artifacts.push({
      kind: "file",
      path: rawPath,
      label: "provider-native-conversation-raw",
      mimeType: "application/json",
      sizeBytes: capture.rawBytes,
      sha256: capture.rawSha256,
    });
    const evidence = `${JSON.stringify(
      {
        schema: "oracle.provider-native-capture-evidence/v1",
        normalization: "text-fields-v1",
        ...summary,
        materializedDocument: {
          path: path.basename(rawPath),
          sha256: capture.rawSha256,
          bytes: capture.rawBytes,
        },
        independentFetch: capture.evidence,
      },
      null,
      2,
    )}\n`;
    const evidencePath = await resolveUniqueArtifactPath(
      path.join(dir, `conversation-${capture.conversationId}-evidence.json`),
    );
    await writeFile(evidencePath, evidence, { encoding: "utf8", mode: 0o600, flag: "wx" });
    artifacts.push({
      kind: "file",
      path: evidencePath,
      label: "provider-native-conversation-evidence",
      mimeType: "application/json",
      sizeBytes: Buffer.byteLength(evidence),
      sha256: createHash("sha256").update(evidence).digest("hex"),
    });
    log(`[capture] Provider-native record saved; answer fidelity: ${summary.answerFidelity}.`);
  } catch {
    summary.failure = {
      reason: summary.status === "captured" ? "write-failed" : "evaluate-failed",
    };
    log("[capture] Provider-native evidence could not be completed; the answer is unaffected.");
  }
  return { summary, artifacts };
}
