import CDP from "chrome-remote-interface";

/** Open a page without activating Chrome, then release the browser connection. */
export async function createBackgroundTarget(options: {
  host: string;
  port: number;
  url: string;
}): Promise<string> {
  const { host, port, url } = options;
  const version = await CDP.Version({ host, port });
  if (!version.webSocketDebuggerUrl) {
    throw new Error("Chrome did not report a browser WebSocket endpoint.");
  }
  const browser = await CDP({ target: version.webSocketDebuggerUrl, local: true });
  try {
    const { targetId } = await browser.Target.createTarget({ url, background: true });
    return targetId;
  } finally {
    await browser.close().catch(() => undefined);
  }
}
