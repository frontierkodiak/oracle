import { describe, expect, test, vi } from "vitest";
import { resolveRunOptionsFromConfig } from "../../src/cli/runOptions.js";
import { shouldDetachSession, stopDetachedWorker } from "../../src/cli/detach.js";

describe("shouldDetachSession", () => {
  test("disables detach when env disables it", () => {
    const result = shouldDetachSession({
      engine: "browser",
      model: "gpt-5-pro",
      waitPreference: true,
      disableDetachEnv: true,
    });
    expect(result).toBe(false);
  });

  test("disables detach for non-pro models (gemini, codex, 5.1)", () => {
    const result = shouldDetachSession({
      engine: "api",
      model: "gemini-3-pro",
      waitPreference: true,
      disableDetachEnv: false,
    });
    expect(result).toBe(false);

    const codex = shouldDetachSession({
      engine: "api",
      model: "gpt-5.1-codex",
      waitPreference: true,
      disableDetachEnv: false,
    });
    expect(codex).toBe(false);

    const standard = shouldDetachSession({
      engine: "api",
      model: "gpt-5.1",
      waitPreference: true,
      disableDetachEnv: false,
    });
    expect(standard).toBe(false);
  });

  test("does not detach pro API runs when wait preference is true", () => {
    const pro52 = shouldDetachSession({
      engine: "api",
      model: "gpt-5.2-pro",
      waitPreference: true,
      disableDetachEnv: false,
    });
    expect(pro52).toBe(false);
  });

  test("allows detach for pro models when wait preference is false and env permits", () => {
    const pro52 = shouldDetachSession({
      engine: "api",
      model: "gpt-5.2-pro",
      waitPreference: false,
      disableDetachEnv: false,
    });
    expect(pro52).toBe(true);
  });

  test.each([true, false])(
    "isolates pro browser runs while wait preference is %s",
    (waitPreference) => {
      const result = shouldDetachSession({
        engine: "browser",
        model: "gpt-5-pro",
        waitPreference,
        disableDetachEnv: false,
      });
      expect(result).toBe(true);
    },
  );

  test.each([true, false])(
    "isolates a resolved GPT-6 Pro browser run while wait preference is %s",
    (waitPreference) => {
      const { resolvedEngine, runOptions } = resolveRunOptionsFromConfig({
        prompt: "Preserve the worker across a foreground interruption",
        engine: "browser",
        model: "gpt-6-pro",
      });
      const policy = {
        engine: resolvedEngine,
        model: runOptions.model!,
        waitPreference,
        disableDetachEnv: false,
      };
      expect(shouldDetachSession(policy)).toBe(true);
      expect(shouldDetachSession({ ...policy, disableDetachEnv: true })).toBe(false);
      expect(shouldDetachSession({ ...policy, model: "gpt-6-astra" })).toBe(false);
      expect(shouldDetachSession({ ...policy, model: "gpt-6-pro-max" })).toBe(false);
      // Browser alias recognition must not add API capabilities or API detach policy.
      expect(shouldDetachSession({ ...policy, engine: "api" })).toBe(false);
    },
  );

  test("keeps non-pro browser runs inline", () => {
    const result = shouldDetachSession({
      engine: "browser",
      model: "gpt-5.6-sol",
      waitPreference: true,
      disableDetachEnv: false,
    });
    expect(result).toBe(false);
  });

  test("stops the detached worker on explicit cancellation", () => {
    const kill = vi.fn();

    expect(stopDetachedWorker(1234, kill)).toBe(true);
    expect(kill).toHaveBeenCalledWith(1234, "SIGTERM");
  });

  test("accepts a worker that already exited during cancellation", () => {
    const error = Object.assign(new Error("missing"), { code: "ESRCH" });

    expect(
      stopDetachedWorker(1234, () => {
        throw error;
      }),
    ).toBe(false);
  });

  test("allows detach for GPT-5.6 Pro reasoning mode", () => {
    const result = shouldDetachSession({
      engine: "api",
      model: "gpt-5.6-sol",
      reasoningMode: "pro",
      waitPreference: false,
      disableDetachEnv: false,
    });
    expect(result).toBe(true);
  });
});
