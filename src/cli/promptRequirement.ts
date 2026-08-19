interface PromptCheckOptions {
  prompt?: string;
  session?: string;
  execSession?: string;
  status?: boolean;
  debugHelp?: boolean;
  route?: boolean;
  preflight?: boolean;
  renderMarkdown?: boolean;
  preview?: boolean | string;
  dryRun?: boolean;
  /** Capture-only browser runs read an existing conversation; there is nothing to ask. */
  browserCaptureOnly?: boolean;
}

/**
 * Determine whether the CLI should enforce a prompt requirement based on raw args and options.
 */
export function shouldRequirePrompt(rawArgs: string[], options: PromptCheckOptions): boolean {
  // A capture-only run submits nothing, so requiring a prompt would force the
  // caller to write a message that must never be sent.
  if (options.browserCaptureOnly) {
    return false;
  }
  if (rawArgs.length === 0) {
    return !options.prompt;
  }
  const firstArg = rawArgs[0];
  const bypassPrompt = Boolean(
    options.session ||
    options.execSession ||
    options.status ||
    options.debugHelp ||
    options.route ||
    options.preflight ||
    firstArg === "status" ||
    firstArg === "session",
  );

  const requiresPrompt =
    options.renderMarkdown || Boolean(options.preview) || Boolean(options.dryRun) || !bypassPrompt;
  return requiresPrompt && !options.prompt;
}
