#!/usr/bin/env bash
set -euo pipefail

echo "== Story 11.2 ML registry smoke =="
echo

mkdir -p .smoke/ml

echo "-- Static schema contract tests"
PYTHONPATH=. pytest pipelines/tests/test_ml_registry_schema_contract.py -q

echo
echo "-- Runtime ML registry schema validation"
python scripts/validate_ml_registry_schema.py

echo
echo "-- Table existence and row counts"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
SELECT
  (
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = 'analytics'
      AND table_name IN ('ml_model_registry', 'ml_predictions')
  )::int || '|' ||
  (SELECT COUNT(*) FROM analytics.ml_model_registry)::int || '|' ||
  (SELECT COUNT(*) FROM analytics.ml_predictions)::int || '|' ||
  (SELECT COUNT(*) FROM analytics.market_features_monthly)::int || '|' ||
  (SELECT COUNT(*) FROM analytics.market_features_monthly WHERE is_trainable)::int;
" > .smoke/ml/ml_registry_counts.txt

cat .smoke/ml/ml_registry_counts.txt

python - <<'PY'
from pathlib import Path

tables, registry_rows, prediction_rows, feature_rows, trainable_rows = [
    int(part)
    for part in Path(".smoke/ml/ml_registry_counts.txt").read_text().strip().split("|")
]

assert tables == 2, f"Expected 2 ML tables, got {tables}"
assert registry_rows == 0, f"Expected empty registry placeholder table, got {registry_rows}"
assert prediction_rows == 0, f"Expected no predictions in Story 11.2, got {prediction_rows}"
assert feature_rows > 0, "Expected market_features_monthly rows"
assert trainable_rows > 0, "Expected trainable market feature rows"

print(
    f"ML tables={tables}, registry_rows={registry_rows}, "
    f"prediction_rows={prediction_rows}, feature_rows={feature_rows}, trainable_rows={trainable_rows}"
)
PY

echo
echo "-- Audit output"
docker compose exec -T postgres psql -U onehaven -d onehaven_market < pipelines/sql/story_11_2_ml_registry_audit.sql

echo
echo "Story 11.2 ML registry smoke passed."
