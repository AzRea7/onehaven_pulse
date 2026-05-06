#!/usr/bin/env bash
set -euo pipefail

export DATABASE_URL="${HOST_DATABASE_URL:-postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market}"

echo "Using host DATABASE_URL=${DATABASE_URL}"

exec "$@"
