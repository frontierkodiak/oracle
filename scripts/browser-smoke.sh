#!/usr/bin/env bash
set -euo pipefail

# Live gate. This script sends real prompts to ChatGPT, so refuse before building or launching anything.
# ChatGPT Pro allowance is scarce: Pro legs run only when a Pro model is named explicitly (no default).
if [ "${ORACLE_LIVE_TEST:-}" != "1" ]; then
  echo "[browser-smoke] refusing to run: this script sends live prompts to ChatGPT. Set ORACLE_LIVE_TEST=1 to opt in." >&2
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
    echo "[browser-smoke] refusing to run: set ORACLE_BROWSER_SMOKE_FAST_MODEL=gpt-5.5-instant (the only allowed fast model; got '${ORACLE_BROWSER_SMOKE_FAST_MODEL:-}')." >&2
    exit 2
    ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CMD=(node "$ROOT/dist/bin/oracle-cli.js" --engine browser --wait --heartbeat 0 --timeout 900 --browser-input-timeout 120000 --browser-model-strategy select)
# A saved browser.thinkingTime of "pro" cannot select Pro on Instant: the strict Pro-effort check aborts the run.
FAST_ARGS=(--model "$FAST_MODEL")
PRO_MODEL="${ORACLE_BROWSER_SMOKE_PRO_MODEL:-}"
if [ -n "$PRO_MODEL" ]; then
  echo "[browser-smoke] WARNING: Pro legs enabled (model: $PRO_MODEL); each Pro leg spends a Pro message from the account allowance."
fi

tmpdir="$(mktemp -d -t oracle-browser-smoke)"
tmpfile="$tmpdir/smoke-attachment.txt"
upload_log="$(mktemp -t oracle-browser-smoke-upload-log)"
trap 'rm -rf "$tmpdir" "$upload_log"' EXIT
echo "smoke-attachment" >"$tmpfile"

echo "[browser-smoke] fast upload attachment (non-inline)"
if ! "${CMD[@]}" "${FAST_ARGS[@]}" --browser-attachments always --prompt "Read the attached file and return exactly one markdown bullet '- upload: <content>' where <content> is the file text." --file "$tmpfile" --slug browser-smoke-upload --force | tee "$upload_log"; then
  exit 1
fi
if ! grep -Eq -- "^[[:space:]]*[-*][[:space:]]+upload:[[:space:]]+smoke-attachment" "$upload_log"; then
  echo "[browser-smoke] upload: expected uploaded file content not found"
  cat "$upload_log"
  exit 1
fi

echo "[browser-smoke] fast simple"
"${CMD[@]}" "${FAST_ARGS[@]}" --prompt "Return exactly one markdown bullet: '- pro-ok'." --slug browser-smoke-pro --force

echo "[browser-smoke] fast with attachment preview (inline)"
"${CMD[@]}" "${FAST_ARGS[@]}" --browser-inline-files --prompt "Read the attached file and return exactly one markdown bullet '- file: <content>' where <content> is the file text." --file "$tmpfile" --slug browser-smoke-file --preview --force

if [ -n "$PRO_MODEL" ]; then
  echo "[browser-smoke] pro standard markdown check"
  "${CMD[@]}" --model "$PRO_MODEL" --prompt "Return two markdown bullets and a fenced code block labeled js that logs 'thinking-ok'." --slug browser-smoke-thinking --force
else
  echo "[browser-smoke] pro standard markdown check: skipped (no Pro model named; set ORACLE_BROWSER_SMOKE_PRO_MODEL)"
fi

if [ -z "$PRO_MODEL" ]; then
  echo "[browser-smoke] reattach flow after controller loss: skipped (no Pro model named; set ORACLE_BROWSER_SMOKE_PRO_MODEL)"
  exit 0
fi

echo "[browser-smoke] reattach flow after controller loss"
slug="browser-reattach-smoke"
meta="$HOME/.oracle/sessions/$slug/meta.json"
logfile="$(mktemp -t oracle-browser-reattach)"
rm -rf "$HOME/.oracle/sessions/$slug"

# Start a browser run in the background and wait until the prompt is submitted.
"${CMD[@]}" --model "$PRO_MODEL" --prompt "Return exactly 'reattach-ok'." --slug "$slug" --browser-keep-browser --heartbeat 0 --timeout 900 --force >"$logfile" 2>&1 &
runner_pid=$!

runtime_ready=0
for _ in {1..40}; do
  if [ -f "$meta" ] && node -e "const fs=require('fs');const p=process.argv[1];const j=JSON.parse(fs.readFileSync(p,'utf8'));if(j.browser?.runtime?.chromePort && j.browser?.runtime?.promptSubmitted === true){process.exit(0);}process.exit(1);" "$meta"; then
    runtime_ready=1
    break
  fi
  sleep 1
done

if [ "$runtime_ready" -ne 1 ]; then
  echo "[browser-smoke] reattach: runtime hint never appeared"
  cat "$logfile"
  kill "$runner_pid" 2>/dev/null || true
  exit 1
fi

# Simulate controller loss.
if ! kill -0 "$runner_pid" 2>/dev/null; then
  echo "[browser-smoke] reattach: controller finished before simulated loss"
  cat "$logfile"
  exit 1
fi
kill "$runner_pid"
wait "$runner_pid" 2>/dev/null || true

if node -e "const fs=require('fs');const j=JSON.parse(fs.readFileSync(process.argv[1],'utf8'));process.exit(j.status === 'completed' ? 0 : 1);" "$meta"; then
  echo "[browser-smoke] reattach: session completed before controller loss"
  cat "$logfile"
  exit 1
fi

reattach_log="$(mktemp -t oracle-browser-reattach-log)"
if ! node "$ROOT/dist/bin/oracle-cli.js" session "$slug" --render-plain >"$reattach_log" 2>&1; then
  echo "[browser-smoke] reattach: session command failed"
  cat "$reattach_log"
  exit 1
fi

if ! grep -q "reattach-ok" "$reattach_log"; then
  echo "[browser-smoke] reattach: expected response not found"
  cat "$reattach_log"
  exit 1
fi
if ! grep -q "Reattach succeeded; session marked completed." "$reattach_log"; then
  echo "[browser-smoke] reattach: command rendered without exercising live reattach"
  cat "$reattach_log"
  exit 1
fi

# Cleanup Chrome if it was left running.
chrome_pid=$(node -e "const fs=require('fs');try{const j=JSON.parse(fs.readFileSync('$meta','utf8'));if(j.browser?.runtime?.chromePid){console.log(j.browser.runtime.chromePid);} }catch{}")
if [ -n "${chrome_pid:-}" ]; then
  kill "$chrome_pid" 2>/dev/null || true
fi
rm -rf "$HOME/.oracle/sessions/$slug" "$logfile" "$reattach_log"
