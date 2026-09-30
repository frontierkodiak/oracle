import { beforeEach, expect, test, vi } from "vitest";

const version = vi.hoisted(() => vi.fn());
const connect = vi.hoisted(() => vi.fn());
vi.mock("chrome-remote-interface", () => ({
  default: Object.assign(connect, { Version: version }),
}));

import { createBackgroundTarget } from "../../src/browser/backgroundTarget.js";
import { openChatGptTarget } from "../../src/browser/liveTabs.js";

beforeEach(() => {
  vi.resetAllMocks();
  version.mockResolvedValue({ webSocketDebuggerUrl: "ws://host:9222/devtools/browser/test" });
});

test("opens on the browser endpoint without activating and closes only the connection", async () => {
  const close = vi.fn().mockResolvedValue(undefined);
  const createTarget = vi.fn().mockResolvedValue({ targetId: "page-1" });
  connect.mockResolvedValue({ Target: { createTarget }, close });

  await expect(openChatGptTarget({ host: "host", port: 9222 })).resolves.toBe("page-1");
  expect(version).toHaveBeenCalledWith({ host: "host", port: 9222 });
  expect(connect).toHaveBeenCalledWith({
    target: "ws://host:9222/devtools/browser/test",
    local: true,
  });
  expect(createTarget).toHaveBeenCalledWith({ url: "https://chatgpt.com/", background: true });
  expect(close).toHaveBeenCalledTimes(1);
});

test("releases the browser connection on creation failure without a foreground fallback", async () => {
  const close = vi.fn().mockResolvedValue(undefined);
  const createTarget = vi.fn().mockRejectedValue(new Error("creation rejected"));
  connect.mockResolvedValue({ Target: { createTarget }, close });

  await expect(
    createBackgroundTarget({ host: "host", port: 9222, url: "about:blank" }),
  ).rejects.toThrow("creation rejected");
  expect(close).toHaveBeenCalledTimes(1);
  expect(connect).toHaveBeenCalledTimes(1);
});

test("does not lose a created target when disconnect cleanup fails", async () => {
  connect.mockResolvedValue({
    Target: { createTarget: vi.fn().mockResolvedValue({ targetId: "page-2" }) },
    close: vi.fn().mockRejectedValue(new Error("already disconnected")),
  });
  await expect(
    createBackgroundTarget({ host: "host", port: 9222, url: "about:blank" }),
  ).resolves.toBe("page-2");
});

test("fails closed when Chrome reports no browser endpoint", async () => {
  version.mockResolvedValue({});
  await expect(
    createBackgroundTarget({ host: "host", port: 9222, url: "about:blank" }),
  ).rejects.toThrow("browser WebSocket endpoint");
  expect(connect).not.toHaveBeenCalled();
});
