import { expect, test, vi } from "vitest";
import { revealManualLoginSetupTab } from "../../src/browser/manualLoginProfile.js";
import type { ChromeClient } from "../../src/browser/types.js";

test.each([
  { headless: false, hideWindow: true, keepBrowser: true },
  { headless: true, hideWindow: false, keepBrowser: true },
  { headless: false, hideWindow: false, keepBrowser: false },
])("does not activate a login tab for unattended settings %j", async (options) => {
  const bringToFront = vi.fn();
  const page = { bringToFront } as unknown as ChromeClient["Page"];
  await expect(revealManualLoginSetupTab(page, options)).resolves.toBe(false);
  expect(bringToFront).not.toHaveBeenCalled();
});

test("activates the actual run tab when the human explicitly keeps visible setup open", async () => {
  const bringToFront = vi.fn().mockResolvedValue(undefined);
  const page = { bringToFront } as unknown as ChromeClient["Page"];
  await expect(
    revealManualLoginSetupTab(page, { headless: false, hideWindow: false, keepBrowser: true }),
  ).resolves.toBe(true);
  expect(bringToFront).toHaveBeenCalledTimes(1);
});
