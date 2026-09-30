import { describe, expect, test } from "vitest";
import { runBrowserMode } from "../../src/browser/index.js";
import type { BrowserLogger } from "../../src/browser/types.js";
import { getCookies } from "@steipete/sweet-cookie";
import { acquireLiveTestLock, releaseLiveTestLock } from "./liveLock.js";

const LIVE = process.env.ORACLE_LIVE_TEST === "1";

async function hasChatGptCookies(): Promise<boolean> {
  const { cookies } = await getCookies({
    url: "https://chatgpt.com",
    origins: ["https://chatgpt.com", "https://chat.openai.com", "https://atlas.openai.com"],
    browsers: ["chrome"],
    mode: "merge",
    chromeProfile: "Default",
    timeoutMs: 5_000,
  });
  // Learned: ChatGPT session token is the most reliable "logged in" signal for live browser tests.
  const hasSession = cookies.some((cookie) =>
    cookie.name.startsWith("__Secure-next-auth.session-token"),
  );
  if (!hasSession) {
    console.warn(
      "Skipping ChatGPT browser live tests (missing __Secure-next-auth.session-token). Open chatgpt.com in Chrome and retry.",
    );
    return false;
  }
  return true;
}

function createLogCapture() {
  const lines: string[] = [];
  const log: BrowserLogger = (message: string) => {
    lines.push(message);
  };
  return { log, lines };
}

function normalizeLabel(label: string): string {
  return label.toLowerCase().replace(/\s+/g, " ").trim();
}

function isMissingChatGptSessionError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /ChatGPT session not detected|Login button detected|login appears missing/i.test(message);
}

// GPT-6 Pro allowance is scarce: the default case is non-Pro. The Pro case runs only when
// ORACLE_LIVE_BROWSER_PRO_MODEL names the Pro model explicitly (each run spends a Pro message).
const PRO_MODEL = process.env.ORACLE_LIVE_BROWSER_PRO_MODEL?.trim();

const CASES = [
  {
    // Instant has no Pro effort tier, so a failed or skipped effort selection cannot leave the run on Pro.
    name: "gpt-5.5-instant",
    desiredModel: "GPT-5.5 Instant",
    thinkingTime: undefined as "pro" | undefined,
    expectedModel: ["5.5", "instant"],
    expectedEffort: [] as string[],
  },
  ...(PRO_MODEL
    ? [
        {
          name: "explicit-pro-effort",
          desiredModel: PRO_MODEL,
          thinkingTime: "pro" as const,
          expectedModel: [] as string[],
          expectedEffort: ["pro"],
        },
      ]
    : []),
];

if (LIVE && !PRO_MODEL) {
  console.warn(
    "Skipping Pro model-selection case (ORACLE_LIVE_BROWSER_PRO_MODEL not set; no Pro model named).",
  );
}

(LIVE ? describe : describe.skip)("ChatGPT browser live model selection", () => {
  test(
    "selects the named model and effort through the Advanced picker (non-Pro by default)",
    async () => {
      if (!(await hasChatGptCookies())) return;
      // Learned: serialize live browser tests to avoid Chrome profile contention.
      await acquireLiveTestLock("chatgpt-browser");
      try {
        for (const entry of CASES) {
          for (let attempt = 1; attempt <= 3; attempt += 1) {
            const { log, lines } = createLogCapture();
            try {
              // Learned: echo the prompt token so we can assert we captured the right assistant turn.
              const promptToken = `live browser ${entry.name}`;
              const result = await runBrowserMode({
                prompt: `${promptToken}\nRepeat the first line exactly. No other text.`,
                config: {
                  chromeProfile: "Default",
                  desiredModel: entry.desiredModel,
                  thinkingTime: entry.thinkingTime,
                  // Pro browser responses (explicit opt-in only) can legitimately take up to ten minutes.
                  timeoutMs: 10 * 60_000,
                },
                log,
              });

              const normalizedAnswer = result.answerText.toLowerCase().replace(/\s+/g, " ").trim();
              const normalizedExpected = promptToken.toLowerCase();
              const truncatedOk =
                normalizedExpected.startsWith(normalizedAnswer) &&
                normalizedAnswer.length >= Math.max(0, normalizedExpected.length - 2);
              expect(normalizedAnswer.includes(normalizedExpected) || truncatedOk).toBe(true);

              const modelLog = lines.find((line) => line.toLowerCase().startsWith("model picker:"));
              expect(modelLog).toBeTruthy();
              if (modelLog) {
                const label = normalizeLabel(modelLog.replace(/^model picker:\s*/i, ""));
                for (const token of entry.expectedModel) {
                  expect(label).toContain(token);
                }
              }
              const effortLog = lines.find((line) =>
                /^(?:\[browser\]\s*)?thinking time:/i.test(line),
              );
              if (entry.thinkingTime) expect(effortLog).toBeTruthy();
              if (effortLog && entry.thinkingTime) {
                const label = normalizeLabel(
                  effortLog.replace(/^(?:\[browser\]\s*)?thinking time:\s*/i, ""),
                );
                for (const token of entry.expectedEffort) {
                  expect(label).toContain(token);
                }
              }
              break;
            } catch (error) {
              const message = error instanceof Error ? error.message : String(error);
              if (isMissingChatGptSessionError(error)) {
                console.warn(
                  "Skipping ChatGPT browser model selection live test (stale ChatGPT session cookie).",
                );
                return;
              }
              if (message.includes("Unable to find model option")) {
                console.warn(
                  `Skipping ${entry.name} model selection (not available for this account): ${message}`,
                );
                break;
              }
              const transient =
                message.includes("Chrome window closed before oracle finished") ||
                message.includes("Prompt did not appear in conversation before timeout") ||
                message.includes("Reattach target did not respond");
              if (transient && attempt < 3) {
                console.warn(
                  `Retrying ${entry.name} model selection (attempt ${attempt + 1}/3): ${message}`,
                );
                await new Promise((resolve) => setTimeout(resolve, 750 * attempt));
                continue;
              }
              throw error;
            }
          }
        }
      } finally {
        await releaseLiveTestLock("chatgpt-browser");
      }
    },
    // Allow all three transient retries to consume the supported ten-minute Pro window.
    35 * 60 * 1000,
  );
});
