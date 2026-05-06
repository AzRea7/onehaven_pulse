#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== Deployment hardening smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo

echo "-- /health"
health_response="$(curl -fsS "${API_BASE_URL}/health")"
echo "${health_response}" | python -m json.tool

echo "-- /ready"
ready_response="$(curl -fsS "${API_BASE_URL}/ready")"
echo "${ready_response}" | python -m json.tool

HEALTH_JSON="${health_response}" READY_JSON="${ready_response}" python - <<'PY'
import json
import os

health = json.loads(os.environ["HEALTH_JSON"])
ready = json.loads(os.environ["READY_JSON"])

assert health["status"] == "healthy"
assert health["database"] == "not_checked"

assert ready["status"] == "ready"
assert ready["database"] == "connected"
assert ready["postgis"]

print("health/readiness checks passed")
PY

echo
echo "-- Docker API must use postgres hostname"
docker compose exec -T api python - <<'PY'
from app.core.config import settings

assert "@postgres:" in settings.database_url, settings.database_url
print("docker DATABASE_URL uses postgres host")
PY

echo
echo "-- Host override must use localhost"
HOST_DATABASE_URL="postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market" \
./scripts/with_host_db.sh python - <<'PY'
import os

database_url = os.environ["DATABASE_URL"]
assert "@localhost:" in database_url, database_url
print("host DATABASE_URL override uses localhost")
PY

echo
echo "-- GZip check"
gzip_headers="$(curl -sS -H "Accept-Encoding: gzip" -D - -o /dev/null "${API_BASE_URL}/admin/source-freshness")"
echo "${gzip_headers}" | grep -i "content-encoding: gzip" >/dev/null
echo "gzip compression present"

echo
echo "-- Production config rejects unsafe localhost CORS/DB"
docker compose exec -T api sh -c '
ENVIRONMENT=production \
DATABASE_URL=postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market \
FRONTEND_ORIGIN=http://localhost:3000 \
CORS_ALLOW_ORIGINS=http://localhost:3000 \
python - <<PY
try:
    from app.core.config import Settings
    Settings()
except Exception as exc:
    message = str(exc)
    assert "production" in message.lower() or "localhost" in message.lower() or "development database password" in message.lower(), message
    print("unsafe production config rejected")
else:
    raise SystemExit("unsafe production config was accepted")
PY
'

echo
echo "Deployment hardening smoke passed."
