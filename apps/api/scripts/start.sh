#!/usr/bin/env sh
set -eu

HOST="${API_HOST:-0.0.0.0}"
PORT="${API_PORT:-8000}"
WORKERS="${API_WORKERS:-1}"
KEEP_ALIVE="${API_TIMEOUT_KEEP_ALIVE:-5}"
RUN_MIGRATIONS_ON_START="${RUN_MIGRATIONS_ON_START:-false}"

if [ "${RUN_MIGRATIONS_ON_START}" = "true" ]; then
  echo "RUN_MIGRATIONS_ON_START=true"
  echo "Running Alembic migrations before API startup..."

  # Retry because Postgres may be healthy at Docker level but still briefly unavailable
  # for a new connection.
  attempts=0
  max_attempts=30

  until alembic upgrade head; do
    attempts=$((attempts + 1))

    if [ "${attempts}" -ge "${max_attempts}" ]; then
      echo "Alembic migrations failed after ${max_attempts} attempts."
      exit 1
    fi

    echo "Alembic migration attempt ${attempts}/${max_attempts} failed. Retrying in 2 seconds..."
    sleep 2
  done

  echo "Alembic migrations completed."
else
  echo "RUN_MIGRATIONS_ON_START=false; skipping Alembic migrations."
fi

exec uvicorn app.main:app \
  --host "${HOST}" \
  --port "${PORT}" \
  --workers "${WORKERS}" \
  --timeout-keep-alive "${KEEP_ALIVE}"
