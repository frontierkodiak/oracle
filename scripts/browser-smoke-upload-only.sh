#!/usr/bin/env bash
set -euo pipefail

# Live gate. This script sends a real prompt to ChatGPT, so refuse before launching anything.
# It must never run on a Pro model: name an explicit non-Pro model (no default).
if [ "${ORACLE_LIVE_TEST:-}" != "1" ]; then
  echo "[browser-smoke-upload-only] refusing to run: this script sends a live prompt to ChatGPT. Set ORACLE_LIVE_TEST=1 to opt in." >&2
  exit 2
fi
# Explicit allowlist: only a model with no Pro effort tier. GPT-5.5 Thinking / GPT-5.4 can start with a Pro
# effort persisted in the picker, and a failed effort selection would keep it and submit on Pro; Oracle has no
# fail-closed mode for non-Pro effort, so the fast legs use Instant and pass no effort. Anything else
# (empty/whitespace, aliases such as classic/latest/gpt-6, Thinking, Pro models) is refused.
FAST_MODEL="$(printf '%s' "${ORACLE_BROWSER_SMOKE_FAST_MODEL:-}" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')"
case "$FAST_MODEL" in
  gpt-5.5-instant) ;;
  *)
    echo "[browser-smoke-upload-only] refusing to run: set ORACLE_BROWSER_SMOKE_FAST_MODEL=gpt-5.5-instant (the only allowed fast model; got '${ORACLE_BROWSER_SMOKE_FAST_MODEL:-}')." >&2
    exit 2
    ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CMD=(node "$ROOT/dist/bin/oracle-cli.js" --engine browser --wait --heartbeat 0 --timeout 900 --browser-input-timeout 120000 --browser-model-strategy select)
# Explicit non-Pro effort so a saved ~/.oracle/config.json browser.thinkingTime (e.g. "pro") cannot be injected.
# "light" is the Instant tier; ensureThinkingTime also aborts if the active effort reads Pro after selection.
FAST_ARGS=(--model "$FAST_MODEL" --browser-thinking-time light)

tmpdir="$(mktemp -d -t oracle-browser-smoke)"
tmpfile="$tmpdir/smoke-attachment.txt"
upload_log="$(mktemp -t oracle-browser-smoke-upload-log)"
trap 'rm -rf "$tmpdir" "$upload_log"' EXIT
echo "smoke-attachment" >"$tmpfile"

echo "[browser-smoke-upload-only] fast upload attachment (non-inline)"
if ! "${CMD[@]}" "${FAST_ARGS[@]}" --browser-attachments always --prompt "Read the attached file and return exactly one markdown bullet '- upload: <content>' where <content> is the file text." --file "$tmpfile" --slug browser-smoke-upload --force | tee "$upload_log"; then
  exit 1
fi
if ! grep -Eq -- "^[[:space:]]*[-*][[:space:]]+upload:[[:space:]]+smoke-attachment" "$upload_log"; then
  echo "[browser-smoke-upload-only] expected uploaded file content not found"
  cat "$upload_log"
  exit 1
fi
