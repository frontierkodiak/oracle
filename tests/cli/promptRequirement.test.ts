import { describe, expect, test } from "vitest";
import { shouldRequirePrompt } from "../../src/cli/promptRequirement.js";

describe("shouldRequirePrompt", () => {
  test("allows status subcommand without prompt", () => {
    const requires = shouldRequirePrompt(["status"], { status: undefined });
    expect(requires).toBe(false);
  });

  test("allows session subcommand without prompt", () => {
    const requires = shouldRequirePrompt(["session", "abc123"], { session: undefined });
    expect(requires).toBe(false);
  });

  test("requires prompt for default run", () => {
    const requires = shouldRequirePrompt(["--model", "gpt-5.1"], {});
    expect(requires).toBe(true);
  });

  test("requires prompt when preview enabled and no positional provided", () => {
    const requires = shouldRequirePrompt([], { preview: "summary" });
    expect(requires).toBe(true);
  });

  test("allows root --session flag without prompt", () => {
    const requires = shouldRequirePrompt(["--session", "abc123"], { session: "abc123" });
    expect(requires).toBe(false);
  });

  test("does not require a prompt for a capture-only browser run", () => {
    const requires = shouldRequirePrompt(
      ["--browser-capture-only", "--chatgpt-url", "https://chatgpt.com/c/abc"],
      { browserCaptureOnly: true },
    );
    expect(requires).toBe(false);
  });

  test("still requires a prompt for an ordinary browser run", () => {
    const requires = shouldRequirePrompt(["--chatgpt-url", "https://chatgpt.com/c/abc"], {});
    expect(requires).toBe(true);
  });
});
