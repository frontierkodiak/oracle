import { expect, test, vi } from "vitest";
import { revealManualLoginSetupTab } from "../../src/browser/manualLoginProfile.js";
import type { ChromeClient } from "../../src/browser/types.js";

test.each([
  { headless: false, hideWindow: false, keepBrowser: true, allowInteractiveLogin: false },
  { headless: false, hideWindow: true, keepBrowser: true },
  { headless: true, hideWindow: false, keepBrowser: true },
  { headless: false, hideWindow: false, keepBrowser: false },
])("does not activate a login tab for unattended settings %j", async (options) => {
  const bringToFront = vi.fn();
  const showWindow = vi.fn();
  const page = { bringToFront } as unknown as ChromeClient["Page"];
  await expect(revealManualLoginSetupTab(page, { ...options, showWindow })).resolves.toBe(false);
  expect(bringToFront).not.toHaveBeenCalled();
  expect(showWindow).not.toHaveBeenCalled();
});

test("activates the actual run tab when the human explicitly keeps visible setup open", async () => {
  const bringToFront = vi.fn().mockResolvedValue(undefined);
  const showWindow = vi.fn().mockResolvedValue(true);
  const page = { bringToFront } as unknown as ChromeClient["Page"];
  await expect(
    revealManualLoginSetupTab(page, {
      headless: false,
      hideWindow: false,
      keepBrowser: true,
      showWindow,
    }),
  ).resolves.toBe(true);
  expect(bringToFront).toHaveBeenCalledTimes(1);
  expect(showWindow).toHaveBeenCalledTimes(1);
});

test("keeps waiting for login when cosmetic activation fails", async () => {
  const bringToFront = vi.fn().mockRejectedValue(new Error("detached"));
  const log = vi.fn();
  await expect(
    revealManualLoginSetupTab({ bringToFront } as unknown as ChromeClient["Page"], {
      headless: false,
      hideWindow: false,
      keepBrowser: true,
      log,
    }),
  ).resolves.toBe(false);
  expect(log).toHaveBeenCalledWith(expect.stringContaining("detached"));
});
