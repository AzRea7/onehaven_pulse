#!/usr/bin/env sh
set -eu

HOST="${API_HOST:-0.0.0.0}"
PORT="${API_PORT:-8000}"
WORKERS="${API_WORKERS:-1}"
KEEP_ALIVE="${API_TIMEOUT_KEEP_ALIVE:-5}"

exec uvicorn app.main:app \
  --host "${HOST}" \
  --port "${PORT}" \
  --workers "${WORKERS}" \
  --timeout-keep-alive "${KEEP_ALIVE}"
