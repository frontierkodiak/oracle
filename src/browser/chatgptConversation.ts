import { createHash } from "node:crypto";
import path from "node:path";
import { mkdir, writeFile } from "node:fs/promises";
import type { SessionArtifact } from "../sessionManager.js";
import { resolveSessionArtifactsDir, resolveUniqueArtifactPath } from "./artifacts.js";
import type { BrowserLogger, ChromeClient } from "./types.js";

/**
 * Provider-native conversation capture.
 *
 * Oracle's answer capture is a rendering of what ChatGPT displayed: copy-button
 * Markdown when it works, DOM text when it does not. Neither is the provider's
 * own record of the conversation, and for notation-heavy answers the difference
 * is not cosmetic — rendered KaTeX loses the LaTeX source it was rendered from.
 *
 * This module fetches ChatGPT's own conversation document from
 * `/backend-api/conversation/<id>` inside the authenticated page, so a caller can
 * hold the provider's bytes rather than a re-rendering of them.
 *
 * Two properties make the result usable as evidence rather than as a second
 * opinion:
 *
 *   A. The document is materialized verbatim — the page returns `response.text()`
 *      and those exact bytes are what gets written and hashed. Nothing is parsed
 *      and re-serialized on the way to disk.
 *   B. A second, independent fetch is normalized and hashed *in the page* before
 *      its same bytes are drained into a separately retained artifact. A
 *      Node-side mistake therefore cannot make B agree with A by construction:
 *      the per-turn evidence is computed before B crosses the boundary.
 *
 * Document-level hashes of A and B are expected to differ: the backend document
 * carries volatile nested metadata that changes between fetches at identical byte
 * length and turn count. That is recorded, never gated. The per-turn comparison is
 * the load-bearing one.
 *
 * Capture is best-effort by design and must never gate an answer. `/backend-api/*`
 * sits behind bot mitigation that can return 403 to an in-page fetch while the
 * user is perfectly well logged in, so every failure is typed and reported rather
 * than thrown.
 */

export type ProviderNativeFailureReason =
  | "no-conversation-id"
  | "auth-session-unavailable"
  | "challenged"
  | "http-error"
  | "empty-document"
  | "evaluate-failed"
  | "digest-unavailable";

export interface ProviderNativeTurnAttachment {
  name: string | null;
  bytes: number | null;
  mimeType: string | null;
}

export interface ProviderNativeTurnDigest {
  /** Position among non-system turns, in conversation order. */
  index: number;
  role: string;
  contentType: string;
  /** UTF-8 byte length of the normalized turn body. */
  bytes: number;
  /** SHA-256 of the normalized turn body, as decimal bytes. */
  sha256Decimal: number[];
  /**
   * Files the provider records against this turn. Deliberately outside the
   * hashed body: an upload does not appear in the turn's content, so without
   * this the evidence cannot say what was sent, and with it inside the body the
   * digest would stop matching the reference normalization.
   */
  attachments?: ProviderNativeTurnAttachment[];
}

export interface ProviderNativeCaptureFailure {
  reason: ProviderNativeFailureReason;
  detail?: string;
  httpStatus?: number;
}

export interface ProviderNativeCapture {
  conversationId: string;
  /** Verbatim bytes of fetch A. */
  rawText: string;
  rawSha256: string;
  rawBytes: number;
  /** Digests derived in-page from the independent fetch B. */
  evidence: {
    documentSha256Decimal: number[];
    documentBytes: number;
    perTurn: ProviderNativeTurnDigest[];
    fetchedAt: string;
  } | null;
  evidenceFailure?: ProviderNativeCaptureFailure;
  /** Verbatim bytes of fetch B, retained separately from authoritative A. */
  independentRawText?: string;
  independentSha256?: string;
  independentBytes?: number;
  /** Recorded, never gated: the backend document mutates between fetches. */
  documentHashesMatch: boolean | null;
}

export type ProviderNativeCaptureOutcome =
  | { status: "captured"; capture: ProviderNativeCapture }
  | { status: "unavailable"; failure: ProviderNativeCaptureFailure };

const STASH_KEY = "__oracleConversationCapture";
const INDEPENDENT_STASH_KEY = "__oracleConversationCaptureIndependent";
const DRAIN_CHUNK_CHARS = 500_000;
/**
 * Ceilings, not guesses about what Chrome will tolerate. A conversation document
 * is normally well under a megabyte; anything past this is a sign the fetch
 * returned something other than a conversation (a challenge page, an error body),
 * and draining it would spend minutes proving that.
 */
const MAX_DOCUMENT_CHARS = 64 * 1024 * 1024;
const CAPTURE_TIMEOUT_MS = 120_000;

async function withTimeout<T>(operation: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** The one canonical provider normalizer used by both the page evidence path and the ledger. */
export function canonicalNormalizeProviderConversation(rawText: string): Array<{
  index: number;
  role: string;
  contentType: string;
  body: string;
  attachments: ProviderNativeTurnAttachment[];
}> {
  const PY_INT = Symbol.for("oracle.pyInt");
  const PY_FLOAT = Symbol.for("oracle.pyFloat");
  let i = 0;
  const error = (message: string): never => {
    throw new Error(`${message} at ${i}`);
  };
  const whitespace = (): void => {
    while (i < rawText.length && " \t\n\r".includes(rawText[i] ?? "")) i += 1;
  };
  const parseString = (): string => {
    const start = i;
    i += 1;
    while (i < rawText.length) {
      const character = rawText[i];
      if (character === "\\") {
        i += 2;
        continue;
      }
      if (character === '"') {
        i += 1;
        return JSON.parse(rawText.slice(start, i)) as string;
      }
      i += 1;
    }
    return error("unterminated string");
  };
  const parseNumber = (): Record<PropertyKey, unknown> => {
    const start = i;
    while (i < rawText.length && "-+.eE0123456789".includes(rawText[i] ?? "")) i += 1;
    const literal = rawText.slice(start, i);
    if (!literal) return error("expected value");
    const value = Number(literal);
    if (!Number.isFinite(value)) return error("invalid number");
    const isFloat = /[.eE]/.test(literal);
    return { [isFloat ? PY_FLOAT : PY_INT]: true, value, literal };
  };
  const parseValue = (): unknown => {
    whitespace();
    const character = rawText[i];
    if (character === "{") return parseObject();
    if (character === "[") return parseArray();
    if (character === '"') return parseString();
    if (rawText.startsWith("true", i)) {
      i += 4;
      return true;
    }
    if (rawText.startsWith("false", i)) {
      i += 5;
      return false;
    }
    if (rawText.startsWith("null", i)) {
      i += 4;
      return null;
    }
    return parseNumber();
  };
  const parseObject = (): Record<string, unknown> => {
    const result: Record<string, unknown> = {};
    i += 1;
    whitespace();
    if (rawText[i] === "}") {
      i += 1;
      return result;
    }
    for (;;) {
      whitespace();
      if (rawText[i] !== '"') return error("expected key");
      const key = parseString();
      whitespace();
      if (rawText[i] !== ":") return error("expected colon");
      i += 1;
      result[key] = parseValue();
      whitespace();
      if (rawText[i] === ",") {
        i += 1;
        continue;
      }
      if (rawText[i] === "}") {
        i += 1;
        return result;
      }
      return error("expected , or }");
    }
  };
  const parseArray = (): unknown[] => {
    const result: unknown[] = [];
    i += 1;
    whitespace();
    if (rawText[i] === "]") {
      i += 1;
      return result;
    }
    for (;;) {
      result.push(parseValue());
      whitespace();
      if (rawText[i] === ",") {
        i += 1;
        continue;
      }
      if (rawText[i] === "]") {
        i += 1;
        return result;
      }
      return error("expected , or ]");
    }
  };
  const isBoxedNumber = (value: unknown): value is Record<PropertyKey, unknown> =>
    value !== null &&
    typeof value === "object" &&
    ((value as Record<PropertyKey, unknown>)[PY_INT] === true ||
      (value as Record<PropertyKey, unknown>)[PY_FLOAT] === true);
  const pyFloatRepr = (value: number): string => {
    const text = String(value);
    return /[.eEn]/.test(text) ? text : `${text}.0`;
  };
  const pyNumber = (value: Record<PropertyKey, unknown>): string => {
    if (value[PY_FLOAT] === true) return pyFloatRepr(value.value as number);
    return String(BigInt(value.literal as string));
  };
  const pyDumps = (value: unknown): string => {
    if (value === null) return "null";
    if (value === true) return "true";
    if (value === false) return "false";
    if (isBoxedNumber(value)) return pyNumber(value);
    if (typeof value === "string") return JSON.stringify(value);
    if (Array.isArray(value)) return `[${value.map(pyDumps).join(", ")}]`;
    if (typeof value === "object" && value !== null) {
      const record = value as Record<string, unknown>;
      return `{${Object.keys(record)
        .sort()
        .map((key) => `${JSON.stringify(key)}: ${pyDumps(record[key])}`)
        .join(", ")}}`;
    }
    return "null";
  };
  const pyStr = (value: unknown): string => {
    if (typeof value === "string") return value;
    if (value === null) return "None";
    if (value === true) return "True";
    if (value === false) return "False";
    if (isBoxedNumber(value)) return pyNumber(value);
    return pyDumps(value);
  };
  const contentText = (content: Record<string, unknown>): [string, string] => {
    const contentType = typeof content.content_type === "string" ? content.content_type : "text";
    if (contentType === "text") {
      const parts = Array.isArray(content.parts) ? content.parts : [];
      return [contentType, parts.filter((part) => typeof part === "string").join("\n\n")];
    }
    if (contentType === "code" || contentType === "execution_output") {
      return [contentType, typeof content.text === "string" ? content.text : ""];
    }
    if (contentType === "thoughts") {
      const thoughts = Array.isArray(content.thoughts) ? content.thoughts : [];
      return [
        contentType,
        thoughts
          .map((thought) => {
            if (
              thought !== null &&
              typeof thought === "object" &&
              !Array.isArray(thought) &&
              !isBoxedNumber(thought)
            ) {
              const inner = (thought as Record<string, unknown>).content;
              return inner === undefined ? "None" : pyStr(inner);
            }
            return pyStr(thought);
          })
          .join("\n\n"),
      ];
    }
    if (contentType === "reasoning_recap") {
      return [contentType, content.content ? pyStr(content.content) : ""];
    }
    if (contentType === "multimodal_text") {
      const parts = Array.isArray(content.parts) ? content.parts : [];
      return [
        contentType,
        parts.map((part) => (typeof part === "string" ? part : pyDumps(part))).join("\n\n"),
      ];
    }
    return [contentType, pyDumps(content)];
  };
  const nodeOrder = (document: Record<string, unknown>): string[] => {
    const mapping = document.mapping as Record<string, Record<string, unknown>>;
    const current = document.current_node;
    const chain: string[] = [];
    const seen = new Set<string>();
    let nodeId = typeof current === "string" && mapping[current] ? current : undefined;
    if (!nodeId) {
      const roots = Object.keys(mapping).filter((key) => !mapping[key]?.parent);
      if (roots.length === 0) throw new Error("backend-api mapping has no root node");
      nodeId = roots[0];
    }
    while (nodeId && mapping[nodeId] && !seen.has(nodeId)) {
      seen.add(nodeId);
      chain.push(nodeId);
      if (typeof mapping[nodeId].parent === "string") nodeId = mapping[nodeId].parent as string;
      else nodeId = undefined;
    }
    if (typeof current !== "string" || !mapping[current]) return chain;
    const reversed: string[] = [];
    for (let index = chain.length - 1; index >= 0; index -= 1) {
      reversed.push(chain[index] as string);
    }
    return reversed;
  };
  const document = parseValue();
  whitespace();
  if (
    i !== rawText.length ||
    !document ||
    typeof document !== "object" ||
    Array.isArray(document)
  ) {
    return error("backend-api JSON has no mapping");
  }
  const record = document as Record<string, unknown>;
  if (!record.mapping || typeof record.mapping !== "object" || Array.isArray(record.mapping)) {
    return error("backend-api JSON has no mapping");
  }
  const mapping = record.mapping as Record<string, Record<string, unknown>>;
  const turns: Array<{
    index: number;
    role: string;
    contentType: string;
    body: string;
    attachments: ProviderNativeTurnAttachment[];
  }> = [];
  for (const nodeId of nodeOrder(record)) {
    const message = mapping[nodeId]?.message;
    if (!message || typeof message !== "object" || Array.isArray(message)) continue;
    const messageRecord = message as Record<string, unknown>;
    const author = messageRecord.author;
    const role =
      author &&
      typeof author === "object" &&
      typeof (author as Record<string, unknown>).role === "string"
        ? ((author as Record<string, unknown>).role as string)
        : "unknown";
    if (role === "system") continue;
    const content =
      messageRecord.content &&
      typeof messageRecord.content === "object" &&
      !Array.isArray(messageRecord.content)
        ? (messageRecord.content as Record<string, unknown>)
        : {};
    const [contentType, body] = contentText(content);
    const metadata = messageRecord.metadata;
    const attachments =
      metadata &&
      typeof metadata === "object" &&
      Array.isArray((metadata as Record<string, unknown>).attachments)
        ? ((metadata as Record<string, unknown>).attachments as unknown[])
            .filter((entry) => entry && typeof entry === "object" && !Array.isArray(entry))
            .map((entry) => {
              const attachment = entry as Record<string, unknown>;
              return {
                name: typeof attachment.name === "string" ? attachment.name : null,
                bytes: typeof attachment.size === "number" ? attachment.size : null,
                mimeType: typeof attachment.mime_type === "string" ? attachment.mime_type : null,
              };
            })
        : [];
    turns.push({ index: turns.length, role, contentType, body, attachments });
  }
  return turns;
}

function buildNormalizerSource(): string {
  // This serializes the same self-contained function used by Node-side ledger
  // validation. It is source generation for the page sandbox, not a production
  // Node eval helper.
  return `const normalizeTurns = ${canonicalNormalizeProviderConversation.toString()};`;
}

function buildAuthAndFetchSource(conversationId: string): string {
  return `
    const conversationId = ${JSON.stringify(conversationId)};
    // The conversation endpoint needs the bearer token that /api/auth/session
    // issues to the logged-in page. The token is used here and never returned.
    const fetchConversationText = async () => {
      const sessionResponse = await fetch('/api/auth/session', { credentials: 'include' });
      if (!sessionResponse.ok) {
        return { ok: false, reason: 'auth-session-unavailable', httpStatus: sessionResponse.status };
      }
      const session = await sessionResponse.json().catch(() => null);
      const accessToken = session && typeof session.accessToken === 'string' ? session.accessToken : null;
      if (!accessToken) {
        return { ok: false, reason: 'auth-session-unavailable', detail: 'session carries no accessToken' };
      }
      const response = await fetch('/backend-api/conversation/' + encodeURIComponent(conversationId), {
        credentials: 'include',
        headers: { Authorization: 'Bearer ' + accessToken, Accept: 'application/json' },
      });
      if (!response.ok) {
        // Bot mitigation answers with an HTML challenge rather than JSON, and it
        // means "retry later from a human-looking page", not "you are logged out".
        const contentType = response.headers.get('content-type') || '';
        const challenged = response.status === 403 || contentType.includes('text/html');
        return {
          ok: false,
          reason: challenged ? 'challenged' : 'http-error',
          httpStatus: response.status,
        };
      }
      const text = await response.text();
      if (!text) return { ok: false, reason: 'empty-document' };
      return { ok: true, text };
    };
  `;
}

function buildFetchDocumentExpression(conversationId: string): string {
  return `(async () => {
    ${buildAuthAndFetchSource(conversationId)}
    const result = await fetchConversationText();
    if (!result.ok) return result;
    // Stashed rather than returned whole: a conversation document can be several
    // megabytes, and one oversized evaluate response is a worse failure mode than
    // a handful of bounded ones.
    globalThis[${JSON.stringify(STASH_KEY)}] = result.text;
    return { ok: true, length: result.text.length };
  })()`;
}

function buildDrainExpression(offset: number, stashKey = STASH_KEY): string {
  return `(() => {
    const stash = globalThis[${JSON.stringify(stashKey)}];
    if (typeof stash !== 'string') return null;
    return stash.slice(${offset}, ${offset + DRAIN_CHUNK_CHARS});
  })()`;
}

function buildReleaseExpression(stashKey = STASH_KEY): string {
  return `(() => { delete globalThis[${JSON.stringify(stashKey)}]; return true; })()`;
}

/**
 * Normalize-and-digest, shared by the live evidence path and its test double.
 * `sourceExpression` must evaluate to `{ok:true,text}` or a typed failure.
 */
function buildDigestSource(sourceExpression: string, stashKey?: string): string {
  return `
    ${buildNormalizerSource()}
    if (!globalThis.crypto || !globalThis.crypto.subtle || typeof globalThis.crypto.subtle.digest !== 'function') {
      return { ok: false, reason: 'digest-unavailable' };
    }
    const result = await (${sourceExpression});
    if (!result.ok) return result;
    if (result.text.length > ${MAX_DOCUMENT_CHARS}) {
      return { ok: false, reason: 'http-error', detail: 'document exceeds capture ceiling' };
    }
    ${stashKey ? `globalThis[${JSON.stringify(stashKey)}] = result.text;` : ""}
    const encoder = new TextEncoder();
    const digestDecimal = async (value) => {
      const bytes = encoder.encode(value);
      const digest = await globalThis.crypto.subtle.digest('SHA-256', bytes);
      return { digest: Array.from(new Uint8Array(digest)), bytes: bytes.length };
    };
    let turns;
    try {
      turns = normalizeTurns(result.text);
    } catch (error) {
      return { ok: false, reason: 'evaluate-failed', detail: String(error && error.message ? error.message : error) };
    }
    const perTurn = [];
    for (const turn of turns) {
      const hashed = await digestDecimal(turn.body);
      perTurn.push({
        index: turn.index,
        role: turn.role,
        contentType: turn.contentType,
        bytes: hashed.bytes,
        sha256Decimal: hashed.digest,
        attachments: turn.attachments,
      });
    }
    const documentDigest = await digestDecimal(result.text);
    return {
      ok: true,
      documentSha256Decimal: documentDigest.digest,
      documentBytes: documentDigest.bytes,
      documentChars: result.text.length,
      perTurn,
      fetchedAt: new Date().toISOString(),
    };
  `;
}

/**
 * Fetch B: independent, normalized and hashed without leaving the page. Its
 * body is stashed only after the digest source has obtained it, then drained in
 * bounded chunks into a separately retained artifact.
 */
function buildEvidenceExpression(conversationId: string): string {
  return `(async () => {
    ${buildAuthAndFetchSource(conversationId)}
    ${buildDigestSource("fetchConversationText()", INDEPENDENT_STASH_KEY)}
  })()`;
}

/**
 * The same normalization and hashing the page performs, over a caller-supplied
 * document instead of a fetched one. Exists so the normalizer can be checked
 * against the reference implementation it must agree with, without a browser.
 */
export function buildNormalizeAndDigestExpressionForTest(rawText: string): string {
  return `(async () => {
    ${buildDigestSource(`Promise.resolve({ ok: true, text: ${JSON.stringify(rawText)} })`)}
  })()`;
}

async function evaluateInPage<T>(
  Runtime: ChromeClient["Runtime"],
  expression: string,
  awaitPromise: boolean,
): Promise<T | null> {
  const evaluated = await withTimeout(
    Runtime.evaluate({ expression, awaitPromise, returnByValue: true }),
    CAPTURE_TIMEOUT_MS,
    "in-page evaluation",
  );
  const exception = (evaluated as { exceptionDetails?: { text?: string } }).exceptionDetails;
  if (exception) {
    throw new Error(exception.text ?? "in-page evaluation threw");
  }
  return (evaluated.result?.value ?? null) as T | null;
}

interface InPageFailure {
  ok: false;
  reason: ProviderNativeFailureReason;
  detail?: string;
  httpStatus?: number;
}

function toFailure(value: InPageFailure): ProviderNativeCaptureFailure {
  return { reason: value.reason, detail: value.detail, httpStatus: value.httpStatus };
}

export async function captureProviderNativeConversation(params: {
  Runtime: ChromeClient["Runtime"];
  conversationId: string | null | undefined;
  logger?: BrowserLogger;
}): Promise<ProviderNativeCaptureOutcome> {
  const { Runtime, logger } = params;
  const conversationId = params.conversationId?.trim();
  if (!conversationId) {
    return { status: "unavailable", failure: { reason: "no-conversation-id" } };
  }

  let head: ({ ok: true; length: number } | InPageFailure) | null;
  try {
    head = await evaluateInPage(Runtime, buildFetchDocumentExpression(conversationId), true);
  } catch (error) {
    return {
      status: "unavailable",
      failure: {
        reason: "evaluate-failed",
        detail: error instanceof Error ? error.message : String(error),
      },
    };
  }
  if (!head) {
    return { status: "unavailable", failure: { reason: "evaluate-failed" } };
  }
  if (!head.ok) {
    return { status: "unavailable", failure: toFailure(head) };
  }
  if (head.length > MAX_DOCUMENT_CHARS) {
    await evaluateInPage(Runtime, buildReleaseExpression(), false).catch(() => null);
    return {
      status: "unavailable",
      failure: {
        reason: "http-error",
        detail: `document of ${head.length} chars exceeds the ${MAX_DOCUMENT_CHARS}-char capture ceiling`,
      },
    };
  }

  let rawText = "";
  try {
    while (rawText.length < head.length) {
      const chunk = await evaluateInPage<string>(
        Runtime,
        buildDrainExpression(rawText.length),
        false,
      );
      if (chunk === null || chunk === "") {
        break;
      }
      rawText += chunk;
    }
  } finally {
    await evaluateInPage(Runtime, buildReleaseExpression(), false).catch(() => null);
  }

  if (rawText.length !== head.length) {
    return {
      status: "unavailable",
      failure: {
        reason: "evaluate-failed",
        detail: `document drained ${rawText.length} of ${head.length} chars`,
      },
    };
  }

  const rawBuffer = Buffer.from(rawText, "utf8");
  const capture: ProviderNativeCapture = {
    conversationId,
    rawText,
    rawSha256: createHash("sha256").update(rawBuffer).digest("hex"),
    rawBytes: rawBuffer.byteLength,
    evidence: null,
    documentHashesMatch: null,
  };

  let evidence:
    | (
        | {
            ok: true;
            documentSha256Decimal: number[];
            documentBytes: number;
            documentChars: number;
            perTurn: ProviderNativeTurnDigest[];
            fetchedAt: string;
          }
        | InPageFailure
      )
    | null;
  try {
    evidence = await evaluateInPage(Runtime, buildEvidenceExpression(conversationId), true);
  } catch (error) {
    evidence = {
      ok: false,
      reason: "evaluate-failed",
      detail: error instanceof Error ? error.message : String(error),
    };
  }

  if (evidence && evidence.ok) {
    capture.evidence = {
      documentSha256Decimal: evidence.documentSha256Decimal,
      documentBytes: evidence.documentBytes,
      perTurn: evidence.perTurn,
      fetchedAt: evidence.fetchedAt,
    };
    let independentText = "";
    try {
      while (independentText.length < evidence.documentChars) {
        const chunk = await evaluateInPage<string>(
          Runtime,
          buildDrainExpression(independentText.length, INDEPENDENT_STASH_KEY),
          false,
        );
        if (chunk === null || chunk === "") break;
        independentText += chunk;
      }
    } catch (error) {
      capture.evidenceFailure = {
        reason: "evaluate-failed",
        detail: error instanceof Error ? error.message : String(error),
      };
    } finally {
      await evaluateInPage(Runtime, buildReleaseExpression(INDEPENDENT_STASH_KEY), false).catch(
        () => null,
      );
    }
    if (!capture.evidenceFailure) {
      const independentBytes = Buffer.from(independentText, "utf8");
      const independentSha256 = createHash("sha256").update(independentBytes).digest("hex");
      if (
        independentText.length !== evidence.documentChars ||
        independentBytes.byteLength !== evidence.documentBytes ||
        independentSha256 !== Buffer.from(evidence.documentSha256Decimal).toString("hex")
      ) {
        capture.evidenceFailure = {
          reason: "evaluate-failed",
          detail: "independent document changed or was truncated while draining",
        };
      } else {
        capture.independentRawText = independentText;
        capture.independentSha256 = independentSha256;
        capture.independentBytes = independentBytes.byteLength;
      }
    }
    const evidenceHex = Buffer.from(evidence.documentSha256Decimal).toString("hex");
    capture.documentHashesMatch = evidenceHex === capture.rawSha256;
    if (!capture.documentHashesMatch) {
      // Expected: the backend document carries volatile nested metadata. Logged
      // so it is visible, recorded so it is auditable, never treated as failure.
      logger?.(
        `[capture] provider document hash differs between fetches (expected: volatile metadata); per-turn digests are the comparison that counts`,
      );
    }
  } else if (evidence) {
    capture.evidenceFailure = toFailure(evidence);
    await evaluateInPage(Runtime, buildReleaseExpression(INDEPENDENT_STASH_KEY), false).catch(
      () => null,
    );
  } else {
    capture.evidenceFailure = { reason: "evaluate-failed" };
    await evaluateInPage(Runtime, buildReleaseExpression(INDEPENDENT_STASH_KEY), false).catch(
      () => null,
    );
  }

  return { status: "captured", capture };
}

/**
 * What a run records about its own provider-native capture: enough to know
 * whether proof-grade material exists and where, without carrying the material.
 */
/**
 * How the run's own captured answer compares to the provider's record of it.
 *
 * `matched` means the answer Oracle captured is byte-identical to one of the
 * turns the provider reports, verified against digests derived by the
 * independent second fetch. `divergent` means it is not — which is not a failed
 * run, but is a run whose transcript must not be treated as the provider's text.
 * Notation is where this bites: a markdown round-trip that renders `_s` as `*s`
 * or drops the escape in `\,` reads fine and is wrong.
 */
export type AnswerFidelity = "matched" | "divergent" | "unknown";

export interface ProviderNativeCaptureSummary {
  status: "captured" | "unavailable";
  /** Whether the run's captured answer matches the provider's own bytes. */
  answerFidelity?: AnswerFidelity;
  /** Which normalization of the captured answer matched, when one did. */
  answerMatch?: "exact" | "trimmed";
  conversationId?: string;
  rawSha256?: string;
  rawBytes?: number;
  turnCount?: number;
  /** Recorded, not gated — the backend document mutates between fetches. */
  documentHashesMatch?: boolean | null;
  failure?: ProviderNativeCaptureFailure;
  evidenceFailure?: ProviderNativeCaptureFailure;
  capturedAt?: string;
}

function decimalToHex(bytes: number[]): string {
  return Buffer.from(bytes).toString("hex");
}

/**
 * Compares the run's captured answer against the provider's turns by digest.
 *
 * Deliberately a digest membership test rather than a second normalizer: the
 * digests come from the independent fetch, so a match is evidence the captured
 * answer is the provider's bytes, and no second implementation of the
 * normalization can drift away from the one that produced them.
 */
function compareAnswerToProviderTurns(
  answerMarkdown: string | undefined,
  perTurn: ProviderNativeTurnDigest[] | undefined,
): { fidelity: AnswerFidelity; match?: "exact" | "trimmed" } {
  if (!answerMarkdown || !perTurn || perTurn.length === 0) {
    return { fidelity: "unknown" };
  }
  const digests = new Set(perTurn.map((turn) => decimalToHex(turn.sha256Decimal)));
  const exact = createHash("sha256").update(Buffer.from(answerMarkdown, "utf8")).digest("hex");
  if (digests.has(exact)) {
    return { fidelity: "matched", match: "exact" };
  }
  // Transcript writers trim; a trailing newline is not a fidelity failure.
  const trimmed = createHash("sha256")
    .update(Buffer.from(answerMarkdown.trim(), "utf8"))
    .digest("hex");
  if (digests.has(trimmed)) {
    return { fidelity: "matched", match: "trimmed" };
  }
  return { fidelity: "divergent" };
}

/**
 * Captures the provider's own conversation document and writes it beside the
 * run's other artifacts, along with the independently-derived digests.
 *
 * Three files rather than one: authoritative raw A, independently fetched raw B,
 * and the evidence file. A downstream verifier can therefore recompute B's
 * document and per-turn hashes without trusting a self-certifying claim.
 *
 * Never throws. A run whose capture failed is still a run whose answer is
 * perfectly good — it simply is not proof-grade, and says so.
 */
export async function finalizeProviderNativeCapture(params: {
  Runtime: ChromeClient["Runtime"];
  conversationId: string | null | undefined;
  conversationUrl?: string | null;
  sessionId?: string;
  /** The answer this run captured, for comparison against the provider's record. */
  answerMarkdown?: string;
  logger?: BrowserLogger;
}): Promise<{ summary: ProviderNativeCaptureSummary; artifacts: SessionArtifact[] }> {
  const { logger } = params;
  let outcome: ProviderNativeCaptureOutcome;
  try {
    outcome = await captureProviderNativeConversation({
      Runtime: params.Runtime,
      conversationId: params.conversationId,
      logger,
    });
  } catch (error) {
    return {
      summary: {
        status: "unavailable",
        failure: {
          reason: "evaluate-failed",
          detail: error instanceof Error ? error.message : String(error),
        },
      },
      artifacts: [],
    };
  }

  if (outcome.status === "unavailable") {
    if (outcome.failure.reason !== "no-conversation-id") {
      logger?.(
        `[capture] Provider-native conversation capture unavailable (${outcome.failure.reason}); the answer is unaffected.`,
      );
    }
    return { summary: { status: "unavailable", failure: outcome.failure }, artifacts: [] };
  }

  const capture = outcome.capture;
  const capturedAt = new Date().toISOString();
  const { fidelity, match } = compareAnswerToProviderTurns(
    params.answerMarkdown,
    capture.evidence?.perTurn,
  );
  if (fidelity === "divergent") {
    logger?.(
      "[capture] The captured answer does not match any provider turn byte-for-byte; treat this transcript as a rendering, not as the provider's text.",
    );
  }
  const summary: ProviderNativeCaptureSummary = {
    status: "captured",
    answerFidelity: fidelity,
    answerMatch: match,
    conversationId: capture.conversationId,
    rawSha256: capture.rawSha256,
    rawBytes: capture.rawBytes,
    turnCount: capture.evidence?.perTurn.length,
    documentHashesMatch: capture.documentHashesMatch,
    evidenceFailure: capture.evidenceFailure,
    capturedAt,
  };

  if (!params.sessionId) {
    return { summary, artifacts: [] };
  }

  const artifacts: SessionArtifact[] = [];
  try {
    const dir = resolveSessionArtifactsDir(params.sessionId);
    await mkdir(dir, { recursive: true });

    const rawPath = await resolveUniqueArtifactPath(
      path.join(dir, `conversation-${capture.conversationId}-raw.json`),
    );
    // Written from the same string that was hashed, so the file on disk is the
    // thing the digest describes.
    await writeFile(rawPath, capture.rawText, "utf8");
    artifacts.push({
      kind: "file",
      path: rawPath,
      label: "provider-native-conversation-raw",
      mimeType: "application/json",
      sizeBytes: capture.rawBytes,
      sha256: capture.rawSha256,
      sourceUrl: params.conversationUrl ?? undefined,
    });

    if (capture.evidence) {
      let independentArtifact: SessionArtifact | undefined;
      if (capture.independentRawText !== undefined) {
        const independentBytes = Buffer.from(capture.independentRawText, "utf8");
        const independentPath = await resolveUniqueArtifactPath(
          path.join(dir, `conversation-${capture.conversationId}-independent.json`),
        );
        await writeFile(independentPath, independentBytes);
        independentArtifact = {
          kind: "file",
          path: independentPath,
          label: "provider-native-conversation-independent",
          mimeType: "application/json",
          sizeBytes: capture.independentBytes ?? independentBytes.byteLength,
          sha256:
            capture.independentSha256 ??
            createHash("sha256").update(independentBytes).digest("hex"),
        };
        artifacts.push(independentArtifact);
      }
      const evidenceDocument = {
        schema: "oracle.provider-native-capture-evidence/v1",
        conversation_id: capture.conversationId,
        chatgpt_url: params.conversationUrl ?? null,
        captured_at: capturedAt,
        fetched_at: capture.evidence.fetchedAt,
        // This descriptor binds the evidence to the exact authoritative raw
        // bytes materialized immediately above.
        materialized_document: {
          sha256: capture.rawSha256,
          bytes: capture.rawBytes,
        },
        independent_document: {
          sha256:
            capture.independentSha256 ??
            capture.evidence.documentSha256Decimal
              .map((value) => value.toString(16).padStart(2, "0"))
              .join(""),
          bytes: capture.independentBytes ?? capture.evidence.documentBytes,
        },
        // The independent second fetch, kept separate on purpose. Its per-turn
        // digests are the evidence; its document hash is only a volatility
        // record. Document-level equality is NOT a fidelity criterion: the
        // backend document carries nested metadata that changes between fetches
        // at identical turn content, so gating on it would fail honest captures
        // and pass nothing extra.
        independent_fetch: {
          document_sha256_decimal_bytes: capture.evidence.documentSha256Decimal.join(" "),
          document_bytes: capture.evidence.documentBytes,
          fetched_at: capture.evidence.fetchedAt,
        },
        document_hashes_match: capture.documentHashesMatch,
        answer_fidelity: fidelity,
        answer_match: match ?? null,
        per_turn: capture.evidence.perTurn.map((turn) => ({
          i: turn.index,
          role: turn.role,
          ct: turn.contentType,
          blen: turn.bytes,
          ...(turn.attachments && turn.attachments.length > 0
            ? { attachments: turn.attachments }
            : {}),
          // Space-separated decimal bytes: the transport-safe encoding verifiers
          // of this format expect, and one that survives copy/paste through
          // channels that mangle hex or JSON arrays.
          sha256_dec: turn.sha256Decimal.join(" "),
          sha256_hex: decimalToHex(turn.sha256Decimal),
        })),
      };
      const evidencePath = await resolveUniqueArtifactPath(
        path.join(dir, `conversation-${capture.conversationId}-evidence.json`),
      );
      const serialized = `${JSON.stringify(evidenceDocument, null, 2)}\n`;
      await writeFile(evidencePath, serialized, "utf8");
      artifacts.push({
        kind: "file",
        path: evidencePath,
        label: "provider-native-conversation-evidence",
        mimeType: "application/json",
        sizeBytes: Buffer.byteLength(serialized, "utf8"),
        sha256: createHash("sha256").update(serialized).digest("hex"),
      });
    }
    logger?.(
      `[capture] Provider-native conversation captured: ${capture.rawBytes} bytes, ${
        capture.evidence?.perTurn.length ?? 0
      } turns independently hashed.`,
    );
  } catch (error) {
    logger?.(
      `[capture] Provider-native conversation captured but could not be written: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }

  return { summary, artifacts };
}
