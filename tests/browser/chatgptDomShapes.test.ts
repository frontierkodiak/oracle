import { readFileSync } from "node:fs";
import { Window } from "happy-dom";
import { afterEach, describe, expect, test } from "vitest";
import {
  buildCompletionVisibilityExpressionForTest,
  buildCopyExpressionForTest,
  buildMarkdownFallbackExtractorForTest,
  classifyTurnTerminal,
  createTerminalGateState,
  readAssistantSnapshot,
  readHighestConversationTurnNumber,
  readSubmittedUserTurnAnchor,
} from "../../src/browser/actions/assistantResponse.js";
import { waitForResumedConversationHydration } from "../../src/browser/actions/navigation.js";
import { readThinkingActivity } from "../../src/browser/actions/thinkingStatus.js";
import { buildConversationTurnCountExpression } from "../../src/browser/conversationTurns.js";
import type { ChromeClient } from "../../src/browser/types.js";

// ChatGPT changed its conversation DOM on 2026-09-25 (PL-157). The new-shape fixtures are the
// `main` element recorded read-only from the live bridge page, with message content scrubbed. The
// old-shape documents are rebuilt from the same messages in the markup the capture code was written
// for (`article[data-testid="conversation-turn-N"]`, `data-message-author-role`, `.markdown`, and a
// test-id action bar inside the turn), so each observer is checked on both shapes of one thread.

const FIXTURE_DIR = new URL("../fixtures/chatgpt-dom/", import.meta.url);
const manifest = JSON.parse(readFileSync(new URL("manifest.json", FIXTURE_DIR), "utf8")) as Record<
  string,
  { url: string }
>;
const FIXTURES = Object.keys(manifest);

interface Message {
  role: "user" | "assistant";
  id: string;
  html: string;
}

const windows: Window[] = [];
afterEach(async () => {
  for (const window of windows.splice(0)) await window.happyDOM.close();
});

function open(html: string, url: string): Window {
  const window = new Window({ url });
  // happy-dom nodes fail `instanceof EventTarget` inside window.eval, which makes the production
  // click dispatcher decline them; a real page has no such gap.
  Object.defineProperty(window.EventTarget, Symbol.hasInstance, {
    value: (value: unknown) =>
      typeof (value as { dispatchEvent?: unknown } | null)?.dispatchEvent === "function",
  });
  window.document.body.innerHTML = html;
  windows.push(window);
  return window;
}

function openNew(name: string): Window {
  return open(readFileSync(new URL(`${name}.html`, FIXTURE_DIR), "utf8"), manifest[name].url);
}

function runtimeFor(window: Window): ChromeClient["Runtime"] {
  return {
    evaluate: async ({ expression }: { expression: string }) => {
      const value = await window.eval(expression);
      return { result: { type: typeof value, value } };
    },
  } as unknown as ChromeClient["Runtime"];
}

const units = (window: Window) =>
  Array.from(window.document.querySelectorAll("[data-content-search-unit-key]"));
const assistantUnits = (window: Window) =>
  units(window).filter((u) =>
    u.getAttribute("data-content-search-unit-key")!.endsWith(":assistant"),
  );
const userUnits = (window: Window) =>
  units(window).filter((u) => u.getAttribute("data-content-search-unit-key")!.endsWith(":user"));
type FixtureElement = NonNullable<ReturnType<Window["document"]["querySelector"]>>;
const bodyOf = (unit: FixtureElement) =>
  unit.querySelector('[data-markdown-text-style="assistant-message"]') as unknown as HTMLElement;

function messagesOf(window: Window): Message[] {
  return units(window).map((unit, index) => {
    const role = unit.getAttribute("data-content-search-unit-key")!.endsWith(":user")
      ? "user"
      : "assistant";
    if (role === "assistant") {
      const body = unit.querySelector("[data-chatgpt-selection-message-id]")!;
      return {
        role,
        id: body.getAttribute("data-chatgpt-selection-message-id")!,
        html: bodyOf(unit).innerHTML,
      };
    }
    return {
      role,
      id: `user-${index}`,
      html: (unit.querySelector('[data-user-message-bubble="true"]') as unknown as HTMLElement)
        .innerHTML,
    };
  });
}

function oldShapeHtml(messages: Message[], options: { finished?: boolean } = {}): string {
  const finished = options.finished ?? true;
  const turns = messages.map((message, index) => {
    const n = index + 1;
    if (message.role === "user") {
      return `<article data-testid="conversation-turn-${n}" data-turn="user"><h5 class="sr-only">You said:</h5><div data-message-author-role="user" data-message-id="${message.id}"><div class="whitespace-pre-wrap">${message.html}</div></div></article>`;
    }
    const bar =
      finished || index < messages.length - 1
        ? '<div class="turn-actions"><button data-testid="copy-turn-action-button" aria-label="Copy"></button><button data-testid="good-response-turn-action-button"></button></div>'
        : "";
    return `<article data-testid="conversation-turn-${n}" data-turn="assistant"><h6 class="sr-only">ChatGPT said:</h6><div data-message-author-role="assistant" data-message-id="${message.id}"><div class="markdown prose">${message.html}</div></div>${bar}</article>`;
  });
  return `<main><div class="thread">${turns.join("")}</div><form><div id="prompt-textarea" contenteditable="true"></div></form></main>`;
}

function openOld(name: string, options: { finished?: boolean } = {}): Window {
  const source = openNew(name);
  return open(oldShapeHtml(messagesOf(source), options), manifest[name].url);
}

// Remove the new-shape assistant action bar(s) that follow the last assistant unit: the answer is
// then still rendering, as far as completion evidence goes.
function stripLastBar(window: Window): void {
  const last = assistantUnits(window).at(-1)!;
  const group = last.closest("[data-content-search-turn-key]")!;
  for (const button of Array.from(group.querySelectorAll("button"))) {
    if (last.contains(button)) continue;
    if (!(last.compareDocumentPosition(button) & 4)) continue;
    button.closest("span")?.remove();
    button.remove();
  }
}

// Append a new turn group (the answer to a newly submitted prompt), cloned from the last group
// with fresh ids and text, as ChatGPT mounts it after send.
function appendNewTurn(window: Window, answer: string): void {
  const groups = window.document.querySelectorAll("[data-turn-key]");
  const last = groups[groups.length - 1];
  const clone = last.cloneNode(true) as unknown as HTMLElement;
  clone.setAttribute("data-turn-key", "new-user-message");
  for (const el of Array.from(clone.querySelectorAll("[data-content-search-unit-key]"))) {
    const key = el.getAttribute("data-content-search-unit-key")!;
    el.setAttribute("data-content-search-unit-key", key.replace(/^[^:]+/, "new-turn"));
  }
  const body = clone.querySelector("[data-chatgpt-selection-message-id]")!;
  body.setAttribute("data-chatgpt-selection-message-id", "new-answer-id");
  bodyOf(clone as unknown as FixtureElement).innerHTML = `<p>${answer}</p>`;
  last.parentElement!.append(clone as never);
}

const cleaned = (text: string) => text.replace(/ /g, " ").trim();

// The long Pro answer is ~300 KB of MathML; happy-dom's innerText over it takes seconds.
describe.each(FIXTURES)("ChatGPT turn DOM, both shapes: %s", { timeout: 60_000 }, (name) => {
  test("turn count sees one node per message on both shapes", async () => {
    const fresh = openNew(name);
    const count = units(fresh).length;
    expect(count).toBeGreaterThanOrEqual(2);
    expect(fresh.eval(buildConversationTurnCountExpression())).toBe(count);
    expect(openOld(name).eval(buildConversationTurnCountExpression())).toBe(count);
  });

  test("resume hydration settles on stable prior turns on both shapes", async () => {
    for (const window of [openNew(name), openOld(name)]) {
      const turns = await waitForResumedConversationHydration(runtimeFor(window), 5_000, () => {}, {
        requirePriorTurns: true,
        requirePromptReady: false,
        expectedConversationUrl: manifest[name].url,
      });
      expect(turns).toBeGreaterThan(0);
    }
  });

  test("the snapshot reads the last answer body with its message id, not the heading", async () => {
    const fresh = openNew(name);
    const lastUnit = assistantUnits(fresh).at(-1)!;
    const expected = cleaned(bodyOf(lastUnit).innerText);
    const expectedId = lastUnit
      .querySelector("[data-chatgpt-selection-message-id]")!
      .getAttribute("data-chatgpt-selection-message-id");

    const snapshot = await readAssistantSnapshot(runtimeFor(fresh));
    expect(snapshot).not.toBeNull();
    expect(cleaned(snapshot!.text ?? "")).toBe(expected);
    expect(snapshot!.text).not.toMatch(/ChatGPT said/);
    expect(snapshot!.messageId).toBe(expectedId);
    expect(snapshot!.turnNumber).toBe(units(fresh).length);
    expect(snapshot!.afterLastUser).toBe(true);

    const old = await readAssistantSnapshot(runtimeFor(openOld(name)));
    expect(cleaned(old!.text ?? "")).toBe(expected);
    expect(old!.messageId).toBe(expectedId);
    expect(old!.turnNumber).toBe(units(fresh).length);
  });

  test("the pre-submit floor rejects the previous answer and admits the next one", async () => {
    const count = units(openNew(name)).length;
    for (const shape of ["new", "old"] as const) {
      const window = shape === "new" ? openNew(name) : openOld(name);
      const runtime = runtimeFor(window);
      const floor = await readHighestConversationTurnNumber(runtime);
      expect(floor).toBe(count);
      // Before the new prompt mounts, the last answer on the page is the previous one.
      expect(await readAssistantSnapshot(runtime, undefined, undefined, floor!)).toBeNull();
      if (shape === "new") {
        appendNewTurn(window, "fresh answer");
        const anchor = await readSubmittedUserTurnAnchor(runtime);
        expect(anchor).toEqual({ turnNumber: floor! + 1, messageId: expect.any(String) });
        const snapshot = await readAssistantSnapshot(runtime, undefined, undefined, floor!);
        expect(snapshot?.text).toBe("fresh answer");
        expect(snapshot?.turnNumber).toBe(floor! + 2);
      }
    }
  });

  test("completion needs the answer's own action bar on both shapes", async () => {
    const fresh = openNew(name);
    const lastUnit = assistantUnits(fresh).at(-1)!;
    const meta = {
      messageId: lastUnit
        .querySelector("[data-chatgpt-selection-message-id]")!
        .getAttribute("data-chatgpt-selection-message-id"),
    };
    const floor = units(fresh).length - 1;
    expect(fresh.eval(buildCompletionVisibilityExpressionForTest(meta, undefined, floor))).toBe(
      true,
    );
    stripLastBar(fresh);
    expect(fresh.eval(buildCompletionVisibilityExpressionForTest(meta, undefined, floor))).toBe(
      false,
    );

    const old = openOld(name);
    expect(old.eval(buildCompletionVisibilityExpressionForTest(meta, undefined, floor))).toBe(true);
    const unfinished = openOld(name, { finished: false });
    expect(
      unfinished.eval(buildCompletionVisibilityExpressionForTest(meta, undefined, floor)),
    ).toBe(false);
  });

  test("the project-view fallback finds the finished answer on both shapes", async () => {
    const fresh = openNew(name);
    const expected = cleaned(bodyOf(assistantUnits(fresh).at(-1)!).innerText);
    const count = units(fresh).length;
    for (const window of [fresh, openOld(name)]) {
      const result = window.eval(`(${buildMarkdownFallbackExtractorForTest("-1")})()`) as {
        text: string;
        turnNumber: number | null;
        completionVisible: boolean;
      };
      expect(cleaned(result.text)).toBe(expected);
      expect(result.turnNumber).toBe(count);
      expect(result.completionVisible).toBe(true);
    }
  });

  test("copy-to-markdown clicks the answer's action-bar Copy on both shapes", async () => {
    for (const shape of ["new", "old"] as const) {
      const window = shape === "new" ? openNew(name) : openOld(name);
      const lastUnit =
        shape === "new"
          ? assistantUnits(window).at(-1)!
          : Array.from(window.document.querySelectorAll('[data-turn="assistant"]')).at(-1)!;
      const messageId =
        shape === "new"
          ? lastUnit
              .querySelector("[data-chatgpt-selection-message-id]")!
              .getAttribute("data-chatgpt-selection-message-id")
          : lastUnit.querySelector("[data-message-id]")!.getAttribute("data-message-id");
      // Every Copy control writes its own marker, so the result names the one that was clicked.
      const all = Array.from(window.document.querySelectorAll('button[aria-label="Copy"]'));
      all.forEach((button, index) =>
        button.addEventListener("click", () => {
          void window.navigator.clipboard.writeText(`copy-${index}`);
        }),
      );
      const barCandidates = all.filter((b) => !lastUnit.contains(b) || shape === "old");
      const barCopy = barCandidates[barCandidates.length - 1];
      const expected = `copy-${all.indexOf(barCopy)}`;
      const result = (await window.eval(buildCopyExpressionForTest({ messageId }))) as {
        success: boolean;
        markdown: string;
      };
      expect(result).toMatchObject({ success: true, markdown: expected });
    }
  });
});

describe("new-shape specifics", () => {
  test("thinking in the reasoning block between the units vetoes completion", async () => {
    const window = openNew("2026-09-25-project-attachment");
    // happy-dom lays nothing out; give every element a box so visibility checks can pass.
    (
      window.HTMLElement.prototype as unknown as { getBoundingClientRect: () => unknown }
    ).getBoundingClientRect = () => ({
      x: 100,
      y: 100,
      left: 100,
      top: 100,
      right: 300,
      bottom: 140,
      width: 200,
      height: 40,
    });
    const floor = units(window).length;
    // The unfinished turn: partial answer text and a transient action bar are already mounted,
    // while the reasoning block (between the user and assistant units) still shimmers.
    appendNewTurn(window, "partial answer");
    const groups = window.document.querySelectorAll("[data-turn-key]");
    const block = groups[groups.length - 1].querySelector(
      "[data-chatgpt-agent-turn-start]",
    )!.parentElement!;
    const shimmer = window.document.createElement("span");
    shimmer.className = "loading-shimmer";
    shimmer.textContent = "Thinking";
    block.append(shimmer);
    const runtime = runtimeFor(window);

    const live = await readThinkingActivity(runtime);
    expect(live.strong).toBe(true);
    const meta = { messageId: "new-answer-id" };
    expect(window.eval(buildCompletionVisibilityExpressionForTest(meta, undefined, floor))).toBe(
      true,
    );
    let gate = createTerminalGateState(0);
    for (let cycle = 0; cycle < 10; cycle += 1) {
      const decision = classifyTurnTerminal(
        gate,
        {
          now: cycle * 1_000,
          len: "partial answer".length,
          contentKey: "new-answer-id::partial answer",
          stopVisible: false,
          barVisible: true,
          strongThinkingActive: live.strong,
        },
        { barConfirmCycles: 3, minStableMs: 1_200 },
      );
      gate = decision.state;
      expect(decision.terminal).toBe(false);
    }

    shimmer.remove();
    expect((await readThinkingActivity(runtime)).strong).toBe(false);
  });

  test("a code block's Copy button inside the answer is not completion evidence", () => {
    const window = openNew("2026-09-25-plain-code-math");
    // Keep only the first turn group: its answer holds two code blocks with their own Copy.
    const groups = Array.from(window.document.querySelectorAll("[data-turn-key]"));
    for (const group of groups.slice(1)) group.remove();
    const unit = assistantUnits(window).at(-1)!;
    expect(unit.querySelectorAll('button[aria-label="Copy"]').length).toBe(2);
    const meta = {
      messageId: unit
        .querySelector("[data-chatgpt-selection-message-id]")!
        .getAttribute("data-chatgpt-selection-message-id"),
    };
    expect(window.eval(buildCompletionVisibilityExpressionForTest(meta, undefined, 1))).toBe(true);
    stripLastBar(window);
    expect(window.eval(buildCompletionVisibilityExpressionForTest(meta, undefined, 1))).toBe(false);
  });

  test("the submitted-prompt anchor carries the user message id", async () => {
    const window = openNew("2026-09-25-project-attachment");
    const anchor = await readSubmittedUserTurnAnchor(runtimeFor(window));
    const lastUser = userUnits(window).at(-1)!;
    const carrier = lastUser.closest("[data-chatgpt-search-message-ids]")!;
    expect(anchor).toEqual({
      turnNumber: units(window).indexOf(lastUser) + 1,
      messageId: carrier.getAttribute("data-chatgpt-search-message-ids"),
    });
  });

  test("culling earlier units lowers the positional ordinal, so the anchor fails closed", async () => {
    const window = openNew("2026-09-25-plain-code-math");
    const runtime = runtimeFor(window);
    const floor = (await readHighestConversationTurnNumber(runtime))!;
    appendNewTurn(window, "fresh answer");
    // ChatGPT unmounts the oldest turn group while the new answer streams.
    window.document.querySelector("[data-turn-key]")!.remove();
    // The new answer now sits at floor + 2 - 2 = floor: rejected, never an older answer admitted.
    expect(await readAssistantSnapshot(runtime, undefined, undefined, floor)).toBeNull();
  });
});
