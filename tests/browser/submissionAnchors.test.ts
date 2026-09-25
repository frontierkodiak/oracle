import { describe, expect, test } from "vitest";
import { copySubmissionAnchors } from "../../src/browser/index.js";

describe("copySubmissionAnchors", () => {
  test("copies a submission's turn anchors into loop baseline state", () => {
    expect(
      copySubmissionAnchors({
        baselineTurns: 5,
        baselineTurnNumber: 5,
        baselineAssistantText: "pre-submit answer",
      }),
    ).toEqual({
      baselineTurns: 5,
      baselineTurnNumber: 5,
      baselineAssistantText: "pre-submit answer",
    });
  });

  // Regression for the follow-up anchoring gap (Opus Deep reproduction): a follow-up must move the
  // ordinal baseline to its own prompt, not keep the first prompt's. All four submission sites call
  // this function, so a broken copy fails here instead of only inside a full browser run.
  test("re-anchors a follow-up on its own prompt ordinal", () => {
    const first = copySubmissionAnchors({
      baselineTurns: 3,
      baselineTurnNumber: 3,
      baselineAssistantText: "answer one",
    });
    const followUp = copySubmissionAnchors({
      baselineTurns: 4,
      baselineTurnNumber: 5,
      baselineAssistantText: "answer two",
    });
    expect(first.baselineTurnNumber).toBe(3);
    expect(followUp).toEqual({
      baselineTurns: 4,
      baselineTurnNumber: 5,
      baselineAssistantText: "answer two",
    });
  });

  test("normalizes an absent ordinal to null instead of reusing a stale baseline", () => {
    expect(
      copySubmissionAnchors({
        baselineTurns: null,
        baselineAssistantText: null,
      }),
    ).toEqual({
      baselineTurns: null,
      baselineTurnNumber: null,
      baselineAssistantText: null,
    });
  });
});
