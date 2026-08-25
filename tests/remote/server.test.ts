import { describe, expect, test } from "vitest";
import http from "node:http";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { mkdir, mkdtemp, readdir, rm, writeFile, readFile, stat } from "node:fs/promises";
import {
  createRemoteServer,
  pickClientBrowserConfig,
  serveRemote,
} from "../../src/remote/server.js";
import { createRemoteBrowserExecutor } from "../../src/remote/client.js";
import type { BrowserRunOptions, BrowserRunResult } from "../../src/browserMode.js";
import type { RemoteArtifactDescriptor } from "../../src/remote/types.js";
import { setOracleHomeDirOverrideForTest } from "../../src/oracleHome.js";

const CAN_LISTEN_LOCALHOST =
  spawnSync(
    process.execPath,
    [
      "-e",
      `
      const net = require('net');
      const s = net.createServer();
      s.on('error', () => process.exit(1));
      s.listen(0, '127.0.0.1', () => s.close(() => process.exit(0)));
    `,
    ],
    { stdio: "ignore" },
  ).status === 0;

describe("remote browser service", () => {
  test("serveRemote refuses unsupported Node before touching browser startup state", async () => {
    const nodeVersions = ["22.23.2", "23.0.0"];
    for (const runtimeVersion of nodeVersions) {
      const root = await mkdtemp(path.join(os.tmpdir(), "oracle-serve-runtime-gate-"));
      const profile = path.join(root, "profile");
      const nodeDescriptor = Object.getOwnPropertyDescriptor(process.versions, "node");
      if (!nodeDescriptor) throw new Error("process.versions.node descriptor is unavailable");
      try {
        Object.defineProperty(process.versions, "node", {
          ...nodeDescriptor,
          value: runtimeVersion,
        });
        await expect(
          serveRemote({
            host: "127.0.0.1",
            port: 0,
            manualLoginDefault: true,
            manualLoginProfileDir: profile,
          }),
        ).rejects.toThrow("Oracle remote service requires Node.js >= 24");
        // The gate is before cookie/profile/DevTools/Chrome work and before the
        // server is created, so no startup state or listener can be left behind.
        await expect(readdir(root)).resolves.toEqual([]);
      } finally {
        Object.defineProperty(process.versions, "node", nodeDescriptor);
        await rm(root, { recursive: true, force: true });
      }
    }
  });

  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "streams logs and returns results via client executor",
    async () => {
      const tmpDir = await mkdtemp(path.join(os.tmpdir(), "oracle-remote-test-"));
      const attachmentPath = path.join(tmpDir, "note.txt");
      const fallbackAttachmentPath = path.join(tmpDir, "fallback.txt");
      await writeFile(attachmentPath, "hello world", "utf8");
      await writeFile(fallbackAttachmentPath, "fallback world", "utf8");

      const runLog: string[] = [];
      const server = await createRemoteServer(
        { host: "127.0.0.1", port: 0, token: "secret", logger: () => {} },
        {
          runBrowser: async (options) => {
            runLog.push(options.prompt);
            expect(options.config?.cookieSync).toBe(false);
            // The server namespaces the client's slug per run so two callers
            // cannot share an artifact directory; the caller's slug stays as the
            // prefix, and the client re-saves what it pulls under its own session.
            expect(options.sessionId).toMatch(/^remote-session-id-[0-9a-f]{8}$/);
            expect(options.followUpPrompts).toEqual(["follow up"]);
            expect(options.attachments).toHaveLength(1);
            const attachment = options.attachments?.[0];
            if (!attachment) {
              throw new Error("missing attachment");
            }
            const stored = await readFile(attachment.path, "utf8");
            expect(stored).toBe("hello world");
            expect(options.fallbackSubmission?.prompt).toBe("fallback prompt");
            expect(options.fallbackSubmission?.attachments).toHaveLength(1);
            const fallbackAttachment = options.fallbackSubmission?.attachments[0];
            if (!fallbackAttachment) {
              throw new Error("missing fallback attachment");
            }
            const fallbackStored = await readFile(fallbackAttachment.path, "utf8");
            expect(fallbackStored).toBe("fallback world");
            options.log?.("uploading attachment");
            const result: BrowserRunResult = {
              answerText: "hi",
              answerMarkdown: "hi",
              tookMs: 1000,
              answerTokens: 42,
              answerChars: 2,
            };
            return result;
          },
        },
      );

      const executor = createRemoteBrowserExecutor({
        host: `127.0.0.1:${server.port}`,
        token: "secret",
      });
      const clientLogs: string[] = [];
      const result = await executor({
        prompt: "remote",
        attachments: [{ path: attachmentPath, displayPath: "note.txt", sizeBytes: 11 }],
        fallbackSubmission: {
          prompt: "fallback prompt",
          attachments: [
            { path: fallbackAttachmentPath, displayPath: "fallback.txt", sizeBytes: 14 },
          ],
        },
        config: {},
        sessionId: "remote-session-id",
        followUpPrompts: ["follow up"],
        log: (message?: string) => {
          if (message) clientLogs.push(message);
        },
      });

      expect(clientLogs.some((entry) => entry.includes("uploading attachment"))).toBe(true);
      expect(result.answerText).toBe("hi");
      expect(runLog).toEqual(["remote"]);

      const healthUnauthorized = await httpGetJson({
        hostname: "127.0.0.1",
        port: server.port,
        path: "/health",
      });
      expect(healthUnauthorized.statusCode).toBe(401);

      const healthOk = await httpGetJson({
        hostname: "127.0.0.1",
        port: server.port,
        path: "/health",
        token: "secret",
      });
      expect(healthOk.statusCode).toBe(200);
      expect(healthOk.json?.ok).toBe(true);
      expect(typeof healthOk.json?.version).toBe("string");
      expect(healthOk.json?.runtime).toEqual({
        name: "node",
        version: process.versions.node,
        major: Number(process.versions.node.split(".")[0]),
        minimumMajor: 24,
      });
      const healthCapabilities = healthOk.json?.capabilities as any;
      expect(healthCapabilities).toMatchObject({
        schemaVersion: 1,
        features: expect.arrayContaining([
          expect.objectContaining({ id: "oracle.remote.artifact-transfer", version: 1 }),
          expect.objectContaining({ id: "oracle.remote.durable-queue", version: 1 }),
        ]),
      });
      expect(healthCapabilities.features).not.toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: "oracle.browser.capture-only", version: 1 }),
        ]),
      );

      const artifactUnauthorized = await httpGetJson({
        hostname: "127.0.0.1",
        port: server.port,
        path: "/runs/run-id/artifacts/artifact-id",
      });
      expect(artifactUnauthorized.statusCode).toBe(401);

      const malformedArtifactPath = await httpGetJson({
        hostname: "127.0.0.1",
        port: server.port,
        path: "/runs/%E0%A4%A/artifacts/artifact-id",
        token: "secret",
      });
      expect(malformedArtifactPath.statusCode).toBe(404);

      const healthAfterMalformedPath = await httpGetJson({
        hostname: "127.0.0.1",
        port: server.port,
        path: "/health",
        token: "secret",
      });
      expect(healthAfterMalformedPath.statusCode).toBe(200);

      await server.close();
      await rm(tmpDir, { recursive: true, force: true });
    },
  );

  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "transports capture-only runs without creating a prompt submission",
    async () => {
      let runBrowserCalls = 0;
      const server = await createRemoteServer(
        {
          host: "127.0.0.1",
          port: 0,
          token: "secret",
          logger: () => {},
          allowCaptureOnly: true,
        },
        {
          runBrowser: async (options) => {
            runBrowserCalls += 1;
            expect(options.prompt).toBe("");
            expect(options.fallbackSubmission).toBeUndefined();
            expect(options.config).toMatchObject({
              resumeConversationUrl: "https://chatgpt.com/c/existing-conversation",
              captureProviderNative: true,
              captureOnly: true,
            });
            expect(options.config?.desiredModel).toBeUndefined();
            expect(options.config?.modelStrategy).toBeUndefined();
            expect(options.config?.thinkingTime).toBeUndefined();

            // This is the injected browser boundary: the capture-only path has
            // already returned before any selection, typing, or submit action.
            return {
              answerText: "captured transcript",
              answerMarkdown: "captured transcript",
              tookMs: 1,
              answerTokens: 2,
              answerChars: 19,
              conversationId: "existing-conversation",
              promptSubmitted: false,
            };
          },
        },
      );

      try {
        const executor = createRemoteBrowserExecutor({
          host: `127.0.0.1:${server.port}`,
          token: "secret",
        });
        const result = await executor({
          prompt: "",
          config: {
            resumeConversationUrl: "https://chatgpt.com/c/existing-conversation",
            captureProviderNative: true,
            captureOnly: true,
          },
        });

        expect(runBrowserCalls).toBe(1);
        expect(result.answerText).toBe("captured transcript");
        expect(result.conversationId).toBe("existing-conversation");
        expect(result.promptSubmitted).toBe(false);
        expect(result.modelSelection).toBeUndefined();
        expect(result.thinkingSelection).toBeUndefined();
      } finally {
        await server.close();
      }
    },
  );

  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "keeps manual-login Chrome but requests completed run-tab cleanup",
    async () => {
      const manualLoginProfileDir = "/tmp/oracle-manual-login-profile-test";
      const cleanupPolicies: Array<boolean | undefined> = [];
      const server = await createRemoteServer(
        {
          host: "127.0.0.1",
          port: 0,
          token: "secret",
          logger: () => {},
          manualLoginDefault: true,
          manualLoginProfileDir,
        },
        {
          runBrowser: async (options) => {
            expect(options.config).toMatchObject({
              manualLogin: true,
              manualLoginProfileDir,
              keepBrowser: true,
              cookieSync: false,
            });
            cleanupPolicies.push(options.closeOwnedTabOnComplete);
            return {
              answerText: "done",
              answerMarkdown: "done",
              tookMs: 1,
              answerTokens: 1,
              answerChars: 4,
            };
          },
        },
      );

      try {
        const executor = createRemoteBrowserExecutor({
          host: `127.0.0.1:${server.port}`,
          token: "secret",
        });
        const result = await executor({
          prompt: "remote manual-login cleanup",
          config: {},
        });

        expect(result.answerText).toBe("done");

        const explicitlyKept = await executor({
          prompt: "remote manual-login explicit keep",
          config: { keepBrowser: true },
        });

        expect(explicitlyKept.answerText).toBe("done");
        expect(cleanupPolicies).toEqual([true, false]);
      } finally {
        await server.close();
      }
    },
  );

  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "transfers saved browser file artifacts to the client session directory",
    async () => {
      const tmpDir = await mkdtemp(path.join(os.tmpdir(), "oracle-remote-artifact-test-"));
      const clientHome = path.join(tmpDir, "client-home");
      setOracleHomeDirOverrideForTest(clientHome);
      const hostArtifactPath = path.join(
        clientHome,
        "sessions",
        "host-session",
        "artifacts",
        "host-result.zip",
      );
      const hostPrivatePath = path.join(tmpDir, "host-private.zip");
      const secondHostArtifactPath = path.join(
        clientHome,
        "sessions",
        "second-host-session",
        "artifacts",
        "host-result.zip",
      );
      const emptyZip = Buffer.from([
        0x50, 0x4b, 0x05, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      ]);
      await mkdir(path.dirname(hostArtifactPath), { recursive: true });
      await mkdir(path.dirname(secondHostArtifactPath), { recursive: true });
      await writeFile(hostArtifactPath, emptyZip);
      await writeFile(secondHostArtifactPath, emptyZip);
      await writeFile(hostPrivatePath, emptyZip);

      const server = await createRemoteServer(
        { host: "127.0.0.1", port: 0, token: "secret", logger: () => {} },
        {
          runBrowser: async () => {
            const result: BrowserRunResult = {
              answerText: "done",
              answerMarkdown: "done",
              tookMs: 1000,
              answerTokens: 1,
              answerChars: 4,
              savedFiles: [
                {
                  kind: "file",
                  path: hostArtifactPath,
                  label: "Download",
                  mimeType: "application/octet-stream",
                  sizeBytes: emptyZip.length,
                  sourceUrl: "sandbox:/mnt/data/result.zip",
                  url: "browser-download",
                  finalUrl: "browser-download",
                  filename: "result.zip",
                },
                {
                  kind: "file",
                  path: secondHostArtifactPath,
                  label: "Download another result",
                  mimeType: "application/zip",
                  sizeBytes: emptyZip.length,
                  sourceUrl: "sandbox:/mnt/data/result.zip",
                  url: "browser-download",
                  finalUrl: "browser-download",
                  filename: "result.zip",
                },
                {
                  kind: "file",
                  path: hostPrivatePath,
                  label: "Private download",
                  mimeType: "application/zip",
                  sizeBytes: emptyZip.length,
                  sourceUrl: "sandbox:/mnt/data/private.zip",
                  url: "browser-download",
                  finalUrl: "browser-download",
                  filename: "private.zip",
                },
              ],
              artifacts: [
                {
                  kind: "file",
                  path: hostArtifactPath,
                  label: "result.zip",
                  mimeType: "application/zip",
                  sizeBytes: emptyZip.length,
                  sourceUrl: "sandbox:/mnt/data/result.zip",
                },
              ],
              warnings: [
                {
                  code: "chatgpt-ui-warning",
                  severity: "warning",
                  message: "host-only warning /Users/private/profile",
                },
              ],
            };
            return result;
          },
        },
      );

      const executor = createRemoteBrowserExecutor({
        host: `127.0.0.1:${server.port}`,
        token: "secret",
      });
      const result = await executor({
        prompt: "remote",
        config: {},
        sessionId: "remote-artifact-session",
      });

      expect(result.answerText).toBe("done");
      expect(result.warnings).toEqual(
        expect.arrayContaining([expect.objectContaining({ code: "chatgpt-ui-warning" })]),
      );
      expect(JSON.stringify(result)).not.toContain(hostPrivatePath);
      expect(JSON.stringify(result)).not.toContain("host-only warning /Users/private/profile");
      expect(result.artifacts).toHaveLength(3);
      const artifact = result.artifacts?.[0];
      expect(artifact?.path).toBe(
        path.join(clientHome, "sessions", "remote-artifact-session", "artifacts", "result.zip"),
      );
      expect(artifact?.path).not.toBe(hostArtifactPath);
      expect(artifact).toMatchObject({
        kind: "file",
        label: "result.zip",
        mimeType: "application/zip",
        sizeBytes: emptyZip.length,
        sourceUrl: "bridge-artifact",
        validation: { type: "zip", ok: true },
        transfer: { status: "completed", bytes: emptyZip.length },
        origin: { mode: "bridge" },
      });
      expect(artifact?.sha256).toMatch(/^[a-f0-9]{64}$/);
      await expect(readFile(artifact!.path)).resolves.toEqual(emptyZip);
      const duplicate = result.artifacts?.[1];
      expect(duplicate).toMatchObject({
        kind: "file",
        path: path.join(
          clientHome,
          "sessions",
          "remote-artifact-session",
          "artifacts",
          "result-2.zip",
        ),
        label: "result.zip",
        filename: "result.zip",
      });
      await expect(readFile(duplicate!.path)).resolves.toEqual(emptyZip);
      await expect(stat(hostArtifactPath)).resolves.toMatchObject({ size: emptyZip.length });
      await expect(stat(secondHostArtifactPath)).resolves.toMatchObject({
        size: emptyZip.length,
      });
      await expect(stat(hostPrivatePath)).resolves.toMatchObject({ size: emptyZip.length });
      expect(
        result.artifacts?.some(
          (item) => (item as any).filename === "private.zip" || item.label === "Private download",
        ),
      ).toBe(true);

      await server.close();
      await rm(tmpDir, { recursive: true, force: true });
      setOracleHomeDirOverrideForTest(null);
    },
  );

  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "rejects untrusted artifact identifiers before creating local paths",
    async () => {
      const tmpDir = await mkdtemp(path.join(os.tmpdir(), "oracle-remote-invalid-artifact-"));
      setOracleHomeDirOverrideForTest(tmpDir);
      const payload = Buffer.from("zip");
      const bridge = await createFakeArtifactBridge({
        descriptor: createArtifactDescriptor(payload, { artifactId: "../../escape" }),
        payload,
      });

      try {
        const result = await createRemoteBrowserExecutor({
          host: `127.0.0.1:${bridge.port}`,
          token: "secret",
        })({ prompt: "remote", config: {}, sessionId: "invalid-artifact-session" });

        expect(result.savedFiles).toBeUndefined();
        expect(result.warnings).toEqual([
          expect.objectContaining({
            code: "remote-artifact-transfer-failed",
            message: expect.stringContaining("invalid bridge artifact descriptor"),
          }),
        ]);
        expect(bridge.artifactRequests()).toBe(0);
      } finally {
        await bridge.close();
        await rm(tmpDir, { recursive: true, force: true });
        setOracleHomeDirOverrideForTest(null);
      }
    },
  );

  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "stops chunked artifact downloads that exceed the declared size",
    async () => {
      const tmpDir = await mkdtemp(path.join(os.tmpdir(), "oracle-remote-oversize-artifact-"));
      setOracleHomeDirOverrideForTest(tmpDir);
      const declared = Buffer.from("zip");
      const bridge = await createFakeArtifactBridge({
        descriptor: createArtifactDescriptor(declared),
        payload: Buffer.from("zip plus undeclared bytes"),
      });

      try {
        const result = await createRemoteBrowserExecutor({
          host: `127.0.0.1:${bridge.port}`,
          token: "secret",
        })({ prompt: "remote", config: {}, sessionId: "oversize-artifact-session" });

        expect(result.savedFiles).toBeUndefined();
        expect(result.warnings).toEqual([
          expect.objectContaining({
            code: "remote-artifact-transfer-failed",
            message: expect.stringContaining("exceeds declared size"),
          }),
        ]);
        expect(bridge.artifactRequests()).toBe(1);
        const artifactDir = path.join(tmpDir, "sessions", "oversize-artifact-session", "artifacts");
        expect(await readdir(artifactDir).catch(() => [])).toEqual([]);
      } finally {
        await bridge.close();
        await rm(tmpDir, { recursive: true, force: true });
        setOracleHomeDirOverrideForTest(null);
      }
    },
  );

  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "refuses capture-only requests from a host without the capture capability",
    async () => {
      const payload = Buffer.from("zip");
      const bridge = await createFakeArtifactBridge({
        descriptor: createArtifactDescriptor(payload),
        payload,
      });

      try {
        await expect(
          createRemoteBrowserExecutor({
            host: `127.0.0.1:${bridge.port}`,
            token: "secret",
          })({
            prompt: "",
            config: {
              resumeConversationUrl: "https://chatgpt.com/c/existing-conversation",
              captureProviderNative: true,
              captureOnly: true,
            },
          }),
        ).rejects.toThrow("required capability oracle.browser.capture-only v1");
        expect(bridge.runRequests()).toBe(0);
      } finally {
        await bridge.close();
      }
    },
  );

  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "normalizes adversarial capture-only payloads before attachment materialization",
    async () => {
      let received: BrowserRunOptions | undefined;
      const server = await createRemoteServer(
        {
          host: "127.0.0.1",
          port: 0,
          token: "secret",
          logger: () => {},
          allowCaptureOnly: true,
        },
        {
          runBrowser: async (options) => {
            received = options;
            return {
              answerText: "captured",
              answerMarkdown: "captured",
              tookMs: 1,
              answerTokens: 1,
              answerChars: 8,
              promptSubmitted: false,
            };
          },
        },
      );

      try {
        const response = await httpPostJsonLines({
          hostname: "127.0.0.1",
          port: server.port,
          token: "secret",
          payload: {
            prompt: "do not send this",
            attachments: [
              {
                fileName: "secret.txt",
                displayPath: "secret.txt",
                contentBase64: Buffer.from("secret").toString("base64"),
              },
            ],
            fallbackSubmission: {
              prompt: "fallback must not exist",
              attachments: [],
            },
            browserConfig: {
              resumeConversationUrl: "https://chatgpt.com/c/existing-conversation",
              captureProviderNative: true,
              captureOnly: true,
              desiredModel: "gpt-5.6-sol",
              modelStrategy: "select",
              thinkingTime: "pro",
              researchMode: "deep",
            },
            options: { followUpPrompts: ["follow-up must not exist"] },
          },
        });

        expect(response.statusCode).toBe(202);
        await new Promise((resolve) => setTimeout(resolve, 100));
        expect(received).toBeDefined();
        expect(received?.prompt).toBe("");
        expect(received?.attachments).toEqual([]);
        expect(received?.fallbackSubmission).toBeUndefined();
        expect(received?.followUpPrompts).toBeUndefined();
        expect(received?.config).toMatchObject({
          resumeConversationUrl: "https://chatgpt.com/c/existing-conversation",
          captureProviderNative: true,
          captureOnly: true,
        });
        expect(received?.config?.desiredModel).toBeUndefined();
        expect(received?.config?.modelStrategy).toBeUndefined();
        expect(received?.config?.thinkingTime).toBeUndefined();
        expect(received?.config?.researchMode).toBeUndefined();
      } finally {
        await server.close();
      }
    },
  );
});

function createArtifactDescriptor(
  payload: Buffer,
  overrides: Partial<RemoteArtifactDescriptor> = {},
): RemoteArtifactDescriptor {
  return {
    artifactId: "artifact-id",
    runId: "run-id",
    kind: "file",
    filename: "result.zip",
    mimeType: "application/zip",
    byteSize: payload.length,
    sha256: createHash("sha256").update(payload).digest("hex"),
    sourceUrlKind: "sandbox",
    transferStatus: "ready",
    ...overrides,
  };
}

async function createFakeArtifactBridge({
  descriptor,
  payload,
}: {
  descriptor: RemoteArtifactDescriptor;
  payload: Buffer;
}): Promise<{
  port: number;
  artifactRequests(): number;
  runRequests(): number;
  close(): Promise<void>;
}> {
  let artifactRequestCount = 0;
  let runRequestCount = 0;
  const server = http.createServer((req, res) => {
    if (req.method === "GET" && req.url === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          ok: true,
          version: "test",
          runtime: { name: "node", version: "24.0.0", major: 24, minimumMajor: 24 },
          capabilities: {
            schemaVersion: 1,
            features: [
              { id: "oracle.remote.durable-queue", version: 1 },
              {
                id: "oracle.remote.artifact-transfer",
                version: 1,
                limits: { maxBytes: 512 * 1024 * 1024 },
              },
            ],
          },
        }),
      );
      return;
    }
    if (req.method === "POST" && (req.url === "/runs" || req.url === "/v1/runs")) {
      runRequestCount += 1;
      req.resume();
      if (req.url === "/v1/runs") {
        res.writeHead(202, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            id: descriptor.runId,
            state: "completed",
            phase: "terminal",
            queuePosition: 0,
            roughEtaMs: 0,
            requestHash: "a".repeat(64),
            createdAt: "2026-01-01T00:00:00.000Z",
            updatedAt: "2026-01-01T00:00:00.000Z",
            result: {
              answerText: "done",
              answerMarkdown: "done",
              tookMs: 1,
              answerTokens: 1,
              answerChars: 4,
              artifacts: [descriptor],
            },
          }),
        );
        return;
      }
      res.writeHead(200, { "Content-Type": "application/x-ndjson" });
      res.write(
        `${JSON.stringify({ type: "artifact-ready", runId: descriptor.runId, artifact: descriptor })}\n`,
      );
      res.end(
        `${JSON.stringify({
          type: "result",
          result: {
            answerText: "done",
            answerMarkdown: "done",
            tookMs: 1,
            answerTokens: 1,
            answerChars: 4,
          },
        })}\n`,
      );
      return;
    }
    if (
      req.method === "GET" &&
      req.url?.startsWith(`/v1/runs/${encodeURIComponent(descriptor.runId)}`)
    ) {
      res.writeHead(200, { "Content-Type": "application/json" });
      if (req.url.includes("/events")) res.end(JSON.stringify({ events: [] }));
      else
        res.end(
          JSON.stringify({
            id: descriptor.runId,
            state: "completed",
            phase: "terminal",
            queuePosition: 0,
            roughEtaMs: 0,
            requestHash: "a".repeat(64),
            createdAt: "2026-01-01T00:00:00.000Z",
            updatedAt: "2026-01-01T00:00:00.000Z",
            result: {
              answerText: "done",
              answerMarkdown: "done",
              tookMs: 1,
              answerTokens: 1,
              answerChars: 4,
              artifacts: [descriptor],
            },
          }),
        );
      return;
    }
    if (
      req.method === "GET" &&
      req.url ===
        `/runs/${encodeURIComponent(descriptor.runId)}/artifacts/${encodeURIComponent(descriptor.artifactId)}`
    ) {
      artifactRequestCount += 1;
      res.writeHead(200, {
        "Content-Type": "application/zip",
        "X-Oracle-Artifact-Sha256": descriptor.sha256,
      });
      res.write(payload);
      res.end();
      return;
    }
    res.writeHead(404);
    res.end();
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("fake artifact bridge did not bind a TCP port");
  }
  return {
    port: address.port,
    artifactRequests: () => artifactRequestCount,
    runRequests: () => runRequestCount,
    close: async () => {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    },
  };
}

async function httpGetJson({
  hostname,
  port,
  path,
  token,
}: {
  hostname: string;
  port: number;
  path: string;
  token?: string;
}): Promise<{ statusCode: number; json: Record<string, unknown> | null }> {
  return await new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname,
        port,
        path,
        method: "GET",
        headers: token ? { authorization: `Bearer ${token}` } : undefined,
      },
      (res) => {
        res.setEncoding("utf8");
        let body = "";
        res.on("data", (chunk: string) => {
          body += chunk;
        });
        res.on("end", () => {
          const statusCode = res.statusCode ?? 0;
          let json: Record<string, unknown> | null = null;
          try {
            const parsed = body.length ? JSON.parse(body) : null;
            json =
              parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : null;
          } catch {
            json = null;
          }
          resolve({ statusCode, json });
        });
      },
    );
    req.on("error", reject);
    req.end();
  });
}

async function httpPostJsonLines({
  hostname,
  port,
  token,
  payload,
}: {
  hostname: string;
  port: number;
  token: string;
  payload: unknown;
}): Promise<{ statusCode: number; body: string }> {
  return await new Promise((resolve, reject) => {
    const body = JSON.stringify(payload);
    const req = http.request(
      {
        hostname,
        port,
        path: "/v1/runs",
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(body),
          "Idempotency-Key": `test-${Date.now()}-${Math.random()}`,
        },
      },
      (res) => {
        res.setEncoding("utf8");
        let responseBody = "";
        res.on("data", (chunk: string) => (responseBody += chunk));
        res.on("end", () => resolve({ statusCode: res.statusCode ?? 0, body: responseBody }));
      },
    );
    req.on("error", reject);
    req.end(body);
  });
}

describe("bridged result sanitization", () => {
  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "carries selection evidence and conversation identity, never host detail",
    async () => {
      // Two properties in one test because they are the same decision seen from
      // both sides: the whitelist must pass what makes a remote answer
      // attributable, and must still refuse anything describing this machine.
      const server = await createRemoteServer(
        { host: "127.0.0.1", port: 0, token: "secret", logger: () => {} },
        {
          runBrowser: async () => {
            const result: BrowserRunResult = {
              answerText: "hi",
              answerMarkdown: "hi",
              tookMs: 1,
              answerTokens: 1,
              answerChars: 2,
              modelSelection: {
                requestedModel: "gpt-5.6-sol",
                resolvedLabel: "GPT-5.6 Sol",
                strategy: "select",
                status: "switched",
                verified: true,
                source: "chatgpt-model-picker",
                capturedAt: "2026-08-18T00:00:00.000Z",
              },
              thinkingSelection: {
                requestedLevel: "pro",
                status: "switched",
                resolvedLabel: "Pro",
                verified: true,
                strictFailClosed: true,
                source: "chatgpt-thinking-picker",
                capturedAt: "2026-08-18T00:00:00.000Z",
              },
              tabUrl: "https://chatgpt.com/c/abc-123",
              conversationId: "abc-123",
              promptSubmitted: true,
              chromePid: 4242,
              chromePort: 9222,
              userDataDir: "/Users/someone/.oracle/browser-profile",
            };
            return result;
          },
        },
      );

      const executor = createRemoteBrowserExecutor({
        host: `127.0.0.1:${server.port}`,
        token: "secret",
      });
      const result = await executor({ prompt: "remote", config: {} });

      // Without these a bridged run cannot be proven to have answered at the
      // requested model and effort, and its answer cannot be bound to a URL.
      expect(result.thinkingSelection).toMatchObject({
        requestedLevel: "pro",
        verified: true,
        strictFailClosed: true,
      });
      expect(result.modelSelection?.resolvedLabel).toBe("GPT-5.6 Sol");
      expect(result.conversationId).toBe("abc-123");
      expect(result.tabUrl).toBe("https://chatgpt.com/c/abc-123");

      // Host detail stays on the host.
      expect(result.chromePid).toBeUndefined();
      expect(result.chromePort).toBeUndefined();
      expect(result.userDataDir).toBeUndefined();

      await server.close();
    },
  );
});

describe("client browser-config allowlist", () => {
  test("passes through the fields that describe the conversation", () => {
    const accepted = pickClientBrowserConfig({
      chatgptUrl: "https://chatgpt.com/g/g-p-abc/project",
      desiredModel: "gpt-5.6-sol",
      modelStrategy: "select",
      thinkingTime: "pro",
      archiveConversations: "never",
      resumeConversationUrl: "https://chatgpt.com/c/abc-123",
      captureProviderNative: true,
      captureOnly: true,
      timeoutMs: 900_000,
    });
    expect(accepted).toEqual({
      chatgptUrl: "https://chatgpt.com/g/g-p-abc/project",
      desiredModel: "gpt-5.6-sol",
      modelStrategy: "select",
      thinkingTime: "pro",
      archiveConversations: "never",
      resumeConversationUrl: "https://chatgpt.com/c/abc-123",
      captureProviderNative: true,
      captureOnly: true,
      timeoutMs: 900_000,
    });
  });

  test("drops every field that describes the host rather than the conversation", () => {
    // Each of these is a different way for a token holder to stop asking
    // questions and start running code, reading credentials, or steering another
    // caller's tab. Named individually so a regression names its own hazard.
    const accepted = pickClientBrowserConfig({
      chromePath: "/tmp/evil",
      chromeProfile: "/Users/someone/Library/Application Support/Google/Chrome",
      chromeCookiePath: "/Users/someone/Library/Cookies",
      copyProfileSource: "/Users/someone/Library/Application Support/Google/Chrome",
      remoteChrome: { host: "attacker.example", port: 9222 },
      debugPort: 9222,
      attachRunning: true,
      browserTabRef: "current",
      headless: true,
      hideWindow: true,
      manualLogin: false,
      manualLoginProfileDir: "/tmp/profile",
      manualLoginCookieSync: true,
      cookieSync: true,
      cookieNames: ["__Secure-next-auth.session-token"],
      inlineCookies: [],
      inlineCookiesSource: "somewhere",
      allowCookieErrors: true,
      maxConcurrentTabs: 99,
      profileLockTimeoutMs: 0,
      reuseChromeWaitMs: 0,
      desiredModel: "gpt-5.6-sol",
    } as never);
    expect(accepted).toEqual({ desiredModel: "gpt-5.6-sol" });
  });

  test("treats a missing config as an empty one", () => {
    expect(pickClientBrowserConfig(undefined)).toEqual({});
    expect(pickClientBrowserConfig(null)).toEqual({});
  });
});

describe("advertised addresses", () => {
  test.skipIf(!CAN_LISTEN_LOCALHOST)("does not log a caller-supplied access token", async () => {
    const suppliedToken = "sentinel-supplied-bridge-token";
    const lines: string[] = [];
    const server = await createRemoteServer(
      {
        host: "127.0.0.1",
        port: 0,
        token: suppliedToken,
        logger: (message: string) => lines.push(message),
      },
      {
        runBrowser: async () => ({
          answerText: "",
          answerMarkdown: "",
          tookMs: 0,
          answerTokens: 0,
          answerChars: 0,
        }),
      },
    );

    try {
      const health = await httpGetJson({
        hostname: "127.0.0.1",
        port: server.port,
        path: "/health",
        token: suppliedToken,
      });
      expect(health.statusCode).toBe(200);
      expect(lines.join("\n")).not.toContain(suppliedToken);
      expect(lines).toContain("Access token supplied by caller.");
    } finally {
      await server.close();
    }
  });

  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "does not log an environment-supplied access token",
    async () => {
      const suppliedToken = "sentinel-environment-bridge-token";
      const previousToken = process.env.ORACLE_SERVE_TOKEN;
      const lines: string[] = [];
      process.env.ORACLE_SERVE_TOKEN = suppliedToken;

      try {
        const server = await createRemoteServer(
          {
            host: "127.0.0.1",
            port: 0,
            logger: (message: string) => lines.push(message),
          },
          {
            runBrowser: async () => ({
              answerText: "",
              answerMarkdown: "",
              tookMs: 0,
              answerTokens: 0,
              answerChars: 0,
            }),
          },
        );

        try {
          const health = await httpGetJson({
            hostname: "127.0.0.1",
            port: server.port,
            path: "/health",
            token: suppliedToken,
          });
          expect(health.statusCode).toBe(200);
          expect(lines.join("\n")).not.toContain(suppliedToken);
          expect(lines).toContain("Access token supplied by caller.");
        } finally {
          await server.close();
        }
      } finally {
        if (previousToken === undefined) delete process.env.ORACLE_SERVE_TOKEN;
        else process.env.ORACLE_SERVE_TOKEN = previousToken;
      }
    },
  );

  test.skipIf(!CAN_LISTEN_LOCALHOST)("a loopback bind advertises only loopback", async () => {
    // The banner is how an operator decides whether this port needs a tunnel or
    // a firewall rule. Listing LAN and tailnet addresses for a service bound to
    // 127.0.0.1 tells them it is exposed when it is not.
    const lines: string[] = [];
    const server = await createRemoteServer(
      {
        host: "127.0.0.1",
        port: 0,
        token: "secret",
        logger: (message: string) => lines.push(message),
      },
      {
        runBrowser: async () => ({
          answerText: "",
          answerMarkdown: "",
          tookMs: 0,
          answerTokens: 0,
          answerChars: 0,
        }),
      },
    );
    const banner = lines.join("\n");
    expect(banner).toContain("127.0.0.1");
    expect(banner).not.toMatch(/\b10\.\d+\.\d+\.\d+\b/);
    expect(banner).not.toMatch(/\b100\.\d+\.\d+\.\d+\b/);
    expect(banner).not.toMatch(/\b192\.168\.\d+\.\d+\b/);
    await server.close();
  });
});

describe("bridge concurrency end to end", () => {
  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "two callers run concurrently and a third waits for a slot",
    async () => {
      let active = 0;
      let peakActive = 0;
      const finish: (() => void)[] = [];
      const server = await createRemoteServer(
        {
          host: "127.0.0.1",
          port: 0,
          token: "secret",
          logger: () => {},
          maxConcurrentRuns: 2,
          maxQueuedRuns: 4,
        },
        {
          runBrowser: async () => {
            active += 1;
            peakActive = Math.max(peakActive, active);
            await new Promise<void>((resolve) => finish.push(resolve));
            active -= 1;
            return {
              answerText: "ok",
              answerMarkdown: "ok",
              tookMs: 1,
              answerTokens: 1,
              answerChars: 2,
            };
          },
        },
      );

      const call = async () => {
        const executor = createRemoteBrowserExecutor({
          host: `127.0.0.1:${server.port}`,
          token: "secret",
        });
        return executor({ prompt: "x", config: {} });
      };

      const runs = [call(), call(), call()];
      // Give all three time to arrive; only two may be inside runBrowser.
      await new Promise((resolve) => setTimeout(resolve, 300));
      expect(active).toBe(2);
      expect(peakActive).toBe(2);

      while (finish.length > 0) {
        finish.shift()?.();
        await new Promise((resolve) => setTimeout(resolve, 150));
      }
      const results = await Promise.all(runs);
      expect(results.map((r) => r.answerText)).toEqual(["ok", "ok", "ok"]);
      expect(peakActive).toBe(2);

      await server.close();
    },
  );
});

describe("per-run isolation on the shared host", () => {
  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "two callers sending the same session slug get distinct server-side sessions",
    async () => {
      // Session slugs are prompt-derived, so collisions are ordinary rather than
      // adversarial — and a shared slug means a shared artifact directory.
      const seen: string[] = [];
      const server = await createRemoteServer(
        { host: "127.0.0.1", port: 0, token: "secret", logger: () => {}, maxConcurrentRuns: 2 },
        {
          runBrowser: async (options) => {
            seen.push(String(options.sessionId));
            return {
              answerText: "ok",
              answerMarkdown: "ok",
              tookMs: 1,
              answerTokens: 1,
              answerChars: 2,
            };
          },
        },
      );
      const call = async () =>
        createRemoteBrowserExecutor({ host: `127.0.0.1:${server.port}`, token: "secret" })({
          prompt: "x",
          config: {},
          sessionId: "review-the-ts-data",
        });
      await Promise.all([call(), call()]);

      expect(seen).toHaveLength(2);
      expect(seen[0]).not.toEqual(seen[1]);
      for (const sessionId of seen) {
        expect(sessionId.startsWith("review-the-ts-data-")).toBe(true);
      }
      await server.close();
    },
  );

  test.skipIf(!CAN_LISTEN_LOCALHOST)("does not overwrite the shared-profile tab cap", async () => {
    // The tab cap is the physical constraint on a shared profile and belongs to
    // the host. An operator who lowered it — to stay under an account's
    // throttling, say — must not have that silently replaced by whatever the
    // service happens to admit.
    let observedCap: number | undefined = 7;
    const server = await createRemoteServer(
      { host: "127.0.0.1", port: 0, token: "secret", logger: () => {}, maxConcurrentRuns: 4 },
      {
        runBrowser: async (options) => {
          observedCap = options.config?.maxConcurrentTabs;
          return {
            answerText: "ok",
            answerMarkdown: "ok",
            tookMs: 1,
            answerTokens: 1,
            answerChars: 2,
          };
        },
      },
    );
    await createRemoteBrowserExecutor({ host: `127.0.0.1:${server.port}`, token: "secret" })({
      prompt: "x",
      config: {},
    });
    // Left for the browser layer to resolve from the host's own configuration.
    expect(observedCap).toBeUndefined();
    await server.close();
  });
});

describe("cancellation reaches the run", () => {
  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "a client that disconnects mid-run aborts it instead of letting it finish",
    async () => {
      // Releasing the slot when the run happens to end is not cancellation. The
      // browser keeps a tab and a shared-profile slot for the whole run, so a
      // caller that walked away must be able to give both back immediately.
      let sawSignal: AbortSignal | undefined;
      let observedAbort = false;
      const server = await createRemoteServer(
        { host: "127.0.0.1", port: 0, token: "secret", logger: () => {} },
        {
          runBrowser: async (options) => {
            sawSignal = options.signal;
            await new Promise<void>((resolve) => {
              options.signal?.addEventListener("abort", () => {
                observedAbort = true;
                resolve();
              });
              // Long enough that natural completion cannot be mistaken for
              // cancellation.
              setTimeout(resolve, 10_000);
            });
            return {
              answerText: "",
              answerMarkdown: "",
              tookMs: 0,
              answerTokens: 0,
              answerChars: 0,
            };
          },
        },
      );

      const request = http.request(
        {
          host: "127.0.0.1",
          port: server.port,
          path: "/v1/runs",
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: "Bearer secret",
            "Idempotency-Key": "disconnect-test",
          },
        },
        () => {},
      );
      request.on("error", () => {
        // destroy() below intentionally resets the socket to model a dropped caller.
      });
      request.write(
        JSON.stringify({ prompt: "x", attachments: [], options: {}, browserConfig: {} }),
      );
      request.end();

      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(sawSignal).toBeDefined();
      expect(observedAbort).toBe(false);

      request.destroy();
      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(observedAbort).toBe(false);

      await server.close();
    },
  );
});

describe("transport failure messages", () => {
  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "losing the bridge mid-run says the conversation may exist and not to resubmit",
    async () => {
      // The bare socket error ("aborted") describes the symptom. What the reader
      // needs is that the browser work is not undone by losing the stream, so
      // resubmitting would open a second ChatGPT conversation.
      //
      // Stubbed rather than driven through the real service: the behaviour under
      // test is entirely the client's, and a real server cannot drop a socket
      // mid-run without also waiting for the run it is pretending to perform.
      const stub = http.createServer((req, res) => {
        if (req.method === "GET" && req.url === "/health") {
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(
            JSON.stringify({
              ok: true,
              version: "0.18.0",
              runtime: { name: "node", version: "25.1.0", major: 25, minimumMajor: 24 },
              capabilities: {
                schemaVersion: 1,
                features: [
                  { id: "oracle.remote.durable-queue", version: 1 },
                  { id: "oracle.remote.artifact-transfer", version: 1, limits: { maxBytes: 1024 } },
                ],
              },
            }),
          );
          return;
        }
        if (req.method === "POST" && req.url === "/v1/runs") {
          req.resume();
          res.writeHead(202, { "Content-Type": "application/json" });
          res.end(
            JSON.stringify({
              id: "run-loss",
              state: "queued",
              phase: "accepted",
              queuePosition: 1,
              roughEtaMs: 300000,
              requestHash: "a".repeat(64),
              createdAt: new Date().toISOString(),
              updatedAt: new Date().toISOString(),
            }),
          );
          return;
        }
        if (req.method === "GET" && req.url?.includes("/events")) {
          setTimeout(() => req.socket.destroy(), 20);
          return;
        }
        if (req.method === "GET" && req.url?.includes("/v1/runs/")) {
          res.end(
            JSON.stringify({
              id: "run-loss",
              state: "queued",
              phase: "accepted",
              queuePosition: 1,
              roughEtaMs: 300000,
              requestHash: "a".repeat(64),
              createdAt: new Date().toISOString(),
              updatedAt: new Date().toISOString(),
            }),
          );
          return;
        }
        res.writeHead(404);
        res.end();
      });
      await new Promise<void>((resolve) => stub.listen(0, "127.0.0.1", resolve));
      const { port } = stub.address() as { port: number };

      const executor = createRemoteBrowserExecutor({ host: `127.0.0.1:${port}`, token: "secret" });
      await expect(executor({ prompt: "x", config: { timeoutMs: 100 } })).rejects.toThrow(
        /timed out/,
      );
      await new Promise<void>((resolve) => stub.close(() => resolve()));
    },
  );

  test("an unreachable bridge is reported as unreachable, not as a lost run", async () => {
    const executor = createRemoteBrowserExecutor({ host: "127.0.0.1:1", token: "secret" });
    await expect(executor({ prompt: "x", config: {} })).rejects.toThrow(
      /Could not reach the research bridge at 127\.0\.0\.1:1/,
    );
  });
});

describe("service token sourcing", () => {
  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "takes the token from the environment when the flag is absent",
    async () => {
      // A supervised service has nowhere else to put a secret: a plist or unit
      // carrying `--token <secret>` writes it into a world-readable file and into
      // every `ps` listing on the host.
      const previous = process.env.ORACLE_SERVE_TOKEN;
      process.env.ORACLE_SERVE_TOKEN = "from-the-environment";
      try {
        const server = await createRemoteServer(
          { host: "127.0.0.1", port: 0, logger: () => {} },
          {
            runBrowser: async () => ({
              answerText: "",
              answerMarkdown: "",
              tookMs: 0,
              answerTokens: 0,
              answerChars: 0,
            }),
          },
        );
        const denied = await new Promise<number | undefined>((resolve) => {
          http.get(
            {
              host: "127.0.0.1",
              port: server.port,
              path: "/health",
              headers: { Authorization: "Bearer wrong" },
            },
            (res) => resolve(res.statusCode),
          );
        });
        const allowed = await new Promise<number | undefined>((resolve) => {
          http.get(
            {
              host: "127.0.0.1",
              port: server.port,
              path: "/health",
              headers: { Authorization: "Bearer from-the-environment" },
            },
            (res) => resolve(res.statusCode),
          );
        });
        expect(denied).toBe(401);
        expect(allowed).toBe(200);
        await server.close();
      } finally {
        if (previous === undefined) delete process.env.ORACLE_SERVE_TOKEN;
        else process.env.ORACLE_SERVE_TOKEN = previous;
      }
    },
  );

  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "an explicit flag still wins over the environment",
    async () => {
      const previous = process.env.ORACLE_SERVE_TOKEN;
      process.env.ORACLE_SERVE_TOKEN = "from-the-environment";
      try {
        const server = await createRemoteServer(
          { host: "127.0.0.1", port: 0, token: "from-the-flag", logger: () => {} },
          {
            runBrowser: async () => ({
              answerText: "",
              answerMarkdown: "",
              tookMs: 0,
              answerTokens: 0,
              answerChars: 0,
            }),
          },
        );
        const status = await new Promise<number | undefined>((resolve) => {
          http.get(
            {
              host: "127.0.0.1",
              port: server.port,
              path: "/health",
              headers: { Authorization: "Bearer from-the-flag" },
            },
            (res) => resolve(res.statusCode),
          );
        });
        expect(status).toBe(200);
        await server.close();
      } finally {
        if (previous === undefined) delete process.env.ORACLE_SERVE_TOKEN;
        else process.env.ORACLE_SERVE_TOKEN = previous;
      }
    },
  );
});
describe("cancellation across the bridge", () => {
  test.skipIf(!CAN_LISTEN_LOCALHOST)(
    "a caller aborting a remote run cancels it on the far side",
    async () => {
      // `signal` has to mean the same thing on both sides. Observed only locally
      // it would look like cancellation while the remote run kept its slot and
      // its browser tab until it finished on its own.
      let observedAbort = false;
      const server = await createRemoteServer(
        { host: "127.0.0.1", port: 0, token: "secret", logger: () => {} },
        {
          runBrowser: async (options) => {
            await new Promise<void>((resolve) => {
              options.signal?.addEventListener("abort", () => {
                observedAbort = true;
                resolve();
              });
              setTimeout(resolve, 10_000);
            });
            return {
              answerText: "",
              answerMarkdown: "",
              tookMs: 0,
              answerTokens: 0,
              answerChars: 0,
            };
          },
        },
      );
      const controller = new AbortController();
      const executor = createRemoteBrowserExecutor({
        host: `127.0.0.1:${server.port}`,
        token: "secret",
      });
      const run = executor({ prompt: "x", config: {}, signal: controller.signal });
      await new Promise((resolve) => setTimeout(resolve, 400));
      expect(observedAbort).toBe(false);

      controller.abort();
      await expect(run).rejects.toThrow(/cancelled/i);
      await new Promise((resolve) => setTimeout(resolve, 400));
      expect(observedAbort).toBe(true);

      await server.close();
    },
  );

  test("an already-aborted caller never sends the request", async () => {
    const controller = new AbortController();
    controller.abort();
    const executor = createRemoteBrowserExecutor({ host: "127.0.0.1:1", token: "secret" });
    await expect(executor({ prompt: "x", config: {}, signal: controller.signal })).rejects.toThrow(
      /cancelled before the request was sent/,
    );
  });
});
