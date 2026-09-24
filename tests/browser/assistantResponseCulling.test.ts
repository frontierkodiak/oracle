import { createContext, Script } from "node:vm";
import { describe, expect, test } from "vitest";
import {
  buildAssistantExtractorForTest,
  buildCompletionVisibilityExpressionForTest,
  buildResponseObserverExpressionForTest,
  readAssistantSnapshot,
  readHighestConversationTurnNumber,
  readSubmittedUserTurnAnchor,
} from "../../src/browser/actions/assistantResponse.js";

// Fixture derived from the real Carbon live tab (PL-95). ChatGPT virtualizes turns: turn 1 was
// unmounted into a height placeholder while turns `conversation-turn-2` .. `conversation-turn-6`
// stayed mounted, and the pre-submit baseline was 5. The answer is turn 6; the earlier answers are
// turns 2 and 4. The `conversation-turn-N` ordinal is stable across that culling, so it is the
// anchor the response wait must use.

class El {
  attrs: Record<string, string>;
  children: El[];
  text: string;
  order: number;
  parent: El | null = null;
  constructor(attrs: Record<string, string> = {}, children: El[] = [], text = "", order = 0) {
    this.attrs = attrs;
    this.children = children;
    this.text = text;
    this.order = order;
    for (const c of children) c.parent = this;
  }
  get dataset(): Record<string, string> {
    return {};
  }
  getAttribute(name: string): string | null {
    return this.attrs[name] ?? null;
  }
  get innerText(): string {
    return this.text;
  }
  get textContent(): string {
    return this.text;
  }
  get innerHTML(): string {
    return this.text;
  }
  get className(): string {
    return this.attrs.class ?? "";
  }
  descendants(): El[] {
    return this.children.flatMap((c) => [c, ...c.descendants()]);
  }
  matches(selector: string): boolean {
    return matchSimple(this, selector);
  }
  querySelector(selector: string): El | null {
    return this.descendants().find((d) => matchSimple(d, selector)) ?? null;
  }
  querySelectorAll(selector: string): El[] {
    return this.descendants().filter((d) => matchSimple(d, selector));
  }
  contains(other: El): boolean {
    return this.descendants().includes(other);
  }
  closest(selector: string): El | null {
    let node: El | null = this;
    while (node) {
      if (matchSimple(node, selector)) return node;
      node = node.parent;
    }
    return null;
  }
  compareDocumentPosition(other: El): number {
    if (other.order > this.order) return 4; // FOLLOWING
    if (other.order < this.order) return 2; // PRECEDING
    return 0;
  }
  append(...nodes: El[]): void {
    for (const node of nodes) {
      node.parent = this;
      this.children.push(node);
    }
  }
}

function matchSimple(el: El, selector: string): boolean {
  return selector.split(",").some((part) => {
    const s = part.trim();
    if (!s) return false;
    if (/^[a-z]+$/.test(s)) return s === (el.attrs.__tag ?? "div");
    const attr = /^\[([a-zA-Z-]+)(?:(\^|\*|=)(?:"([^"]*)")?)?\]$/.exec(s);
    if (attr) {
      const [, name, op, value] = attr;
      const actual = el.attrs[name];
      if (actual == null) return false;
      if (!op) return true;
      if (op === "=") return actual === value;
      if (op === "^") return actual.startsWith(value ?? "");
      return actual.includes(value ?? "");
    }
    const classAttr = /^\[class\*="([^"]*)"\]$/.exec(s);
    if (classAttr) return (el.attrs.class ?? "").includes(classAttr[1]);
    if (s.startsWith(".")) return (el.attrs.class ?? "").split(/\s+/).includes(s.slice(1));
    const tagAttr = /^([a-z]+)\[([a-zA-Z-]+)/.exec(s);
    if (tagAttr) return el.attrs.__tag === tagAttr[1] && el.attrs[tagAttr[2]] != null;
    return false;
  });
}

function makeTurn(
  num: number,
  role: string,
  text: string,
  order: number,
  messageId: string,
  hasBar = false,
): El {
  const markdown = new El({ class: "markdown", __tag: "div" }, [], text, order + 0.1);
  const messageChildren: El[] = [markdown];
  if (hasBar) {
    messageChildren.push(
      new El({ "data-testid": "copy-turn-action-button", __tag: "button" }, [], "", order + 0.2),
    );
  }
  const message = new El(
    { "data-message-author-role": role, "data-message-id": messageId, __tag: "div" },
    messageChildren,
    "",
    order + 0.05,
  );
  return new El(
    { "data-testid": `conversation-turn-${num}`, "data-turn": role, __tag: "section" },
    [message],
    "",
    order,
  );
}

function makeDocument(turns: El[]) {
  return {
    querySelectorAll: (selector: string): El[] => {
      if (selector.includes("conversation-turn")) return turns;
      if (
        selector.includes('data-message-author-role="user"') ||
        selector.includes('data-turn="user"')
      ) {
        return turns.filter((t) => (t.attrs["data-turn"] ?? "") === "user");
      }
      return [];
    },
    querySelector: (selector: string): El | null => {
      if (selector.includes("conversation-turn")) return turns[0] ?? null;
      return null;
    },
    body: new El({ __tag: "body" }),
  };
}

function baseContext(document: ReturnType<typeof makeDocument>) {
  return {
    Array,
    Number,
    String,
    JSON,
    Boolean,
    RegExp,
    Math,
    HTMLElement: El,
    HTMLProgressElement: class {},
    document,
    location: { href: "https://chatgpt.com/c/6ab560ec-13f8-83ea-8f4a-5c5b971f5b99" },
  };
}

function runSync(expression: string, document: ReturnType<typeof makeDocument>): unknown {
  return new Script(expression).runInContext(createContext(baseContext(document)));
}

// Run the production observer expression over a fake DOM through the real in-page path
// (MutationObserver + timers + Date are stubbed so the settle loop terminates). Resolves with the
// captured snapshot, or `{ timeout: true }` when nothing is accepted within `raceMs`.
async function runObserver(
  expression: string,
  document: ReturnType<typeof makeDocument>,
  raceMs = 250,
): Promise<unknown> {
  let now = 0;
  const context = createContext({
    ...baseContext(document),
    MutationObserver: class {
      observe() {}
      disconnect() {}
    },
    // Fire short settle timers immediately, but never the expression's long watchdog timeout, so
    // the "nothing accepted" case stays pending for the host-side race.
    setTimeout: (callback: () => void, ms?: number) => {
      if ((ms ?? 0) <= 1_000) callback();
      return 1;
    },
    clearTimeout: () => {},
    Date: { now: () => (now += 1_000) },
    window: {
      getComputedStyle: () => ({ display: "block", visibility: "visible", opacity: "1" }),
      innerHeight: 900,
      innerWidth: 1440,
    },
  });
  return Promise.race([
    new Script(expression).runInContext(context, { timeout: 1_500 }) as Promise<unknown>,
    new Promise((resolve) => setTimeout(() => resolve({ timeout: true }), raceMs)),
  ]);
}

const MOUNTED_TURNS = [
  makeTurn(2, "assistant", "answer one", 0, "m2", true),
  makeTurn(3, "user", "prompt two", 1, "m3"),
  makeTurn(4, "assistant", "answer two", 2, "m4", true),
  makeTurn(5, "user", "prompt three", 3, "m5"),
  makeTurn(6, "assistant", "answer three", 4, "m6", true),
];
const CONVERSATION_ID = "6ab560ec-13f8-83ea-8f4a-5c5b971f5b99";

describe("culling-proof turn-ordinal anchor (PL-95 live fixture)", () => {
  test("the extractor reports the answer's own ordinal and document order", () => {
    const extractor = buildAssistantExtractorForTest("extractFromTurns");
    const extracted = runSync(
      `(() => { ${extractor} return extractFromTurns(); })()`,
      makeDocument(MOUNTED_TURNS),
    ) as { turnNumber?: number; afterLastUser?: boolean; text?: string };
    expect(extracted.text).toBe("answer three");
    expect(extracted.turnNumber).toBe(6);
    expect(extracted.afterLastUser).toBe(true);
  });

  test("the response observer accepts the answer even though its positional index fell below the baseline", async () => {
    const observer = buildResponseObserverExpressionForTest(1_500, 5, CONVERSATION_ID, 5);
    const result = (await runObserver(observer, makeDocument(MOUNTED_TURNS))) as {
      text?: string;
      turnNumber?: number;
    };
    expect(result).not.toHaveProperty("timeout");
    expect(result.text).toBe("answer three");
    expect(result.turnNumber).toBe(6);
  });

  test("completion correlation accepts the higher-ordinal answer and rejects the older one", () => {
    const mounted = makeDocument(MOUNTED_TURNS);
    const baseline5 = buildCompletionVisibilityExpressionForTest({}, 5, 5);
    expect(runSync(baseline5, mounted)).toBe(true);

    // Baseline above the answer's ordinal must reject even though the bar is present.
    const baseline7 = buildCompletionVisibilityExpressionForTest({}, 5, 7);
    expect(runSync(baseline7, mounted)).toBe(false);

    // Scrolled-away hole: only the previous user turn and its answer are mounted, so document
    // order alone would accept the older answer; the ordinal anchor must still reject it.
    const hole = makeDocument([
      makeTurn(3, "user", "prompt two", 0, "m3"),
      makeTurn(4, "assistant", "answer two", 1, "m4", true),
    ]);
    expect(runSync(baseline5, hole)).toBe(false);
  });

  test("readAssistantSnapshot enforces the ordinal anchor", async () => {
    const runtimeFor = (value: unknown) => ({
      evaluate: async () => ({ result: { value } }),
    });
    const answer = { text: "answer three", turnNumber: 6, afterLastUser: true };
    const older = { text: "answer two", turnNumber: 4, afterLastUser: true };
    expect(await readAssistantSnapshot(runtimeFor(answer) as never, 5, undefined, 5)).toEqual(
      answer,
    );
    expect(await readAssistantSnapshot(runtimeFor(older) as never, 5, undefined, 5)).toBeNull();
    // Without an ordinal bound it still falls back to the index/document-order rule.
    expect(await readAssistantSnapshot(runtimeFor(older) as never, 5)).toEqual(older);
  });

  test("the page expressions carry the ordinal bound", () => {
    expect(buildResponseObserverExpressionForTest(1_000, 5, CONVERSATION_ID, 5)).toContain(
      "MIN_TURN_NUMBER = 5",
    );
    expect(buildCompletionVisibilityExpressionForTest({}, 5, 5)).toContain("MIN_TURN_NUMBER = 5");
  });

  test("the commit-time anchor and its floor read the mounted ordinals", async () => {
    const runtimeFor = (document: ReturnType<typeof makeDocument>) => ({
      evaluate: async ({ expression }: { expression: string }) => ({
        result: {
          value: new Script(expression).runInContext(
            createContext({ ...baseContext(document), Number, String, Array, RegExp }),
          ),
        },
      }),
    });
    const mounted = makeDocument(MOUNTED_TURNS);
    // Floor: highest mounted ordinal is the new answer (6) here; with only 2 and 4 mounted it is 4.
    expect(await readHighestConversationTurnNumber(runtimeFor(mounted) as never)).toBe(6);
    expect(
      await readHighestConversationTurnNumber(
        runtimeFor(makeDocument(MOUNTED_TURNS.slice(0, 3))) as never,
      ),
    ).toBe(4);
    // The commit anchor reads the last mounted user turn: turn 5, message m5.
    expect(await readSubmittedUserTurnAnchor(runtimeFor(mounted) as never)).toEqual({
      turnNumber: 5,
      messageId: "m5",
    });
  });

  // Regression for the follow-up anchoring gap (Opus Deep reproduction). With an in-run follow-up
  // pending, the previous answer is the last mounted assistant turn. If the follow-up is bounded by
  // the FIRST prompt's ordinal, that previous answer passes; re-anchoring on the follow-up prompt's
  // own ordinal rejects it.
  test("a stale first-prompt anchor accepts the previous follow-up answer; the re-anchored one rejects it", async () => {
    const followUpFixture = makeDocument([
      makeTurn(1, "user", "first prompt", 0, "m1"),
      makeTurn(2, "assistant", "answer one", 1, "m2", true),
      makeTurn(3, "user", "follow-up prompt", 2, "m3"),
    ]);

    // First prompt's anchor (ordinal 1): the previous answer (turn 2) is accepted.
    const stale = (await runObserver(
      buildResponseObserverExpressionForTest(1_500, 3, CONVERSATION_ID, 1),
      followUpFixture,
    )) as { text?: string; turnNumber?: number };
    expect(stale.text).toBe("answer one");
    expect(stale.turnNumber).toBe(2);

    // Re-anchored on the follow-up prompt (ordinal 3): no accepted snapshot until a new answer
    // with a larger ordinal streams in.
    const reanchored = (await runObserver(
      buildResponseObserverExpressionForTest(1_500, 3, CONVERSATION_ID, 3),
      followUpFixture,
    )) as { timeout?: boolean };
    expect(reanchored).toEqual({ timeout: true });
  });
});
