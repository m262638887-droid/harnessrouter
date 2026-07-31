#!/bin/bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

export CLINE_PROXY_HOST="${CLINE_PROXY_HOST:-0.0.0.0}"
export CLINE_PROXY_PORT="${CLINE_PROXY_PORT:-3015}"
# Direct or via worker: leave default or set CLINE_UPSTREAM
# export CLINE_UPSTREAM="https://worker.example/?url=https://api.cline.bot/api/v1"
export CLINE_UPSTREAM="${CLINE_UPSTREAM:-https://api.cline.bot/api/v1}"
export CLINE_CLIENT_TYPE="${CLINE_CLIENT_TYPE:-cline-cli}"
export CLINE_CLIENT_VERSION="${CLINE_CLIENT_VERSION:-3.0.38}"
export CLINE_PLATFORM="${CLINE_PLATFORM:-cli}"
export CLINE_PLATFORM_VERSION="${CLINE_PLATFORM_VERSION:-3.0.38}"
export CLINE_CORE_VERSION="${CLINE_CORE_VERSION:-0.2.0}"
export CLINE_TASK_ID="${CLINE_TASK_ID:-new-api}"

pkill -f "$DIR/proxy.py" 2>/dev/null || true
sleep 0.3
nohup python3 -u "$DIR/proxy.py" >>"$DIR/proxy.log" 2>&1 &
echo $! >"$DIR/proxy.pid"
sleep 0.4
echo "cline-proxy started pid=$(cat "$DIR/proxy.pid") http://${CLINE_PROXY_HOST}:${CLINE_PROXY_PORT}"
echo "  upstream: $CLINE_UPSTREAM"
echo "  auth: pass-through Bearer from channel keys"
