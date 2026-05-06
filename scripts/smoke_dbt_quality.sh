#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-${REPO_ROOT}/dbt}"
export DBT_TARGET="${DBT_TARGET:-local}"
export DBT_POSTGRES_HOST="${DBT_POSTGRES_HOST:-localhost}"
export DBT_POSTGRES_PORT="${DBT_POSTGRES_PORT:-5432}"
export DBT_POSTGRES_USER="${DBT_POSTGRES_USER:-onehaven}"
export DBT_POSTGRES_PASSWORD="${DBT_POSTGRES_PASSWORD:-onehaven_dev_password}"
export DBT_POSTGRES_DB="${DBT_POSTGRES_DB:-onehaven_market}"

echo "== OneHaven dbt quality gate =="
echo "REPO_ROOT=${REPO_ROOT}"
echo "DBT_TARGET=${DBT_TARGET}"
echo "DBT_POSTGRES_HOST=${DBT_POSTGRES_HOST}"
echo "DBT_POSTGRES_DB=${DBT_POSTGRES_DB}"
echo "DBT_PROFILES_DIR=${DBT_PROFILES_DIR}"
echo

cd "${REPO_ROOT}/dbt"

dbt debug
dbt seed --full-refresh
dbt build --fail-fast --no-partial-parse

echo
echo "dbt quality gate passed."
