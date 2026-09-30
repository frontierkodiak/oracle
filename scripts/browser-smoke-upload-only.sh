#!/usr/bin/env bash
set -euo pipefail

# Live gate. This script sends a real prompt to ChatGPT, so refuse before launching anything.
# It must never run on a Pro model: name an explicit non-Pro model (no default).
if [ "${ORACLE_LIVE_TEST:-}" != "1" ]; then
  echo "[browser-smoke-upload-only] refusing to run: this script sends a live prompt to ChatGPT. Set ORACLE_LIVE_TEST=1 to opt in." >&2
  exit 2
fi
if [ -z "${ORACLE_BROWSER_SMOKE_FAST_MODEL:-}" ]; then
  echo "[browser-smoke-upload-only] refusing to run: set ORACLE_BROWSER_SMOKE_FAST_MODEL to an explicit non-Pro model (e.g. gpt-5.5)." >&2
  exit 2
fi
case "$(printf '%s' "$ORACLE_BROWSER_SMOKE_FAST_MODEL" | tr '[:upper:]' '[:lower:]')" in
  *pro*)
    echo "[browser-smoke-upload-only] refusing to run: ORACLE_BROWSER_SMOKE_FAST_MODEL must not be a Pro model." >&2
    exit 2
    ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CMD=(node "$ROOT/dist/bin/oracle-cli.js" --engine browser --wait --heartbeat 0 --timeout 900 --browser-input-timeout 120000)
FAST_MODEL="$ORACLE_BROWSER_SMOKE_FAST_MODEL"

tmpdir="$(mktemp -d -t oracle-browser-smoke)"
tmpfile="$tmpdir/smoke-attachment.txt"
upload_log="$(mktemp -t oracle-browser-smoke-upload-log)"
trap 'rm -rf "$tmpdir" "$upload_log"' EXIT
echo "smoke-attachment" >"$tmpfile"

echo "[browser-smoke-upload-only] fast upload attachment (non-inline)"
if ! "${CMD[@]}" --model "$FAST_MODEL" --browser-attachments always --prompt "Read the attached file and return exactly one markdown bullet '- upload: <content>' where <content> is the file text." --file "$tmpfile" --slug browser-smoke-upload --force | tee "$upload_log"; then
  exit 1
fi
if ! grep -Eq -- "^[[:space:]]*[-*][[:space:]]+upload:[[:space:]]+smoke-attachment" "$upload_log"; then
  echo "[browser-smoke-upload-only] expected uploaded file content not found"
  cat "$upload_log"
  exit 1
fi
