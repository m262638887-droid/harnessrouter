#!/bin/bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# ---- config (override via env) ----
export PYTHONUNBUFFERED=1
export HR_API_BASE="${HR_API_BASE:-https://api.harnessrouter.ai}"
export HR_OUTBOUND="${HR_OUTBOUND:-}"   # optional: https://your-worker.example/ ?url=  prefix; empty = direct
export HR_KEYS_FILE="${HR_KEYS_FILE:-$DIR/keys.txt}"
export HR_STATE_FILE="${HR_STATE_FILE:-$DIR/harness_state.json}"
export HR_MAX_INPUT_CHARS="${HR_MAX_INPUT_CHARS:-800000}"
export HR_MAX_INPUT_CHARS_CLAUDE="${HR_MAX_INPUT_CHARS_CLAUDE:-220000}"
export HR_MAX_INPUT_CHARS_HERMES="${HR_MAX_INPUT_CHARS_HERMES:-800000}"
export HR_MAX_SYSTEM_CHARS="${HR_MAX_SYSTEM_CHARS:-220000}"
export HR_MAX_MSG_CHARS="${HR_MAX_MSG_CHARS:-100000}"
export HR_MAX_ASSISTANT_HIST_CHARS="${HR_MAX_ASSISTANT_HIST_CHARS:-30000}"
export HR_DEFAULT_MAX_OUTPUT="${HR_DEFAULT_MAX_OUTPUT:-8192}"
export HR_LONG_CTX_CHARS="${HR_LONG_CTX_CHARS:-80000}"
export HR_HEARTBEAT_SECS="${HR_HEARTBEAT_SECS:-8}"
export HR_THIN_MODE="${HR_THIN_MODE:-1}"
export HR_TRUE_STREAM="${HR_TRUE_STREAM:-0}"
export PROXY_PORT="${PROXY_PORT:-18790}"
export PROXY_HOST="${PROXY_HOST:-0.0.0.0}"
export PROXY_TOKEN="${PROXY_TOKEN:-sk-hr-proxy-change-me}"

if [[ ! -s "$HR_KEYS_FILE" ]] || ! grep -q '^sk-hr-' "$HR_KEYS_FILE" 2>/dev/null; then
  echo "[warn] $HR_KEYS_FILE has no sk-hr- keys. Add keys (one per line) then restart."
fi

pkill -f "$DIR/hr_openai_proxy.py" 2>/dev/null || true
sleep 0.5
mkdir -p "$DIR"
touch "$DIR/proxy.log"
nohup python3 -u "$DIR/hr_openai_proxy.py" >>"$DIR/proxy.log" 2>&1 &
echo $! >"$DIR/proxy.pid"
sleep 0.6
echo "hr-proxy started pid=$(cat "$DIR/proxy.pid") http://${PROXY_HOST}:${PROXY_PORT}"
echo "  client Authorization: Bearer ${PROXY_TOKEN}"
echo "  keys file: $HR_KEYS_FILE"
if command -v curl >/dev/null; then
  curl -sS "http://127.0.0.1:${PROXY_PORT}/health" || true
  echo
fi
