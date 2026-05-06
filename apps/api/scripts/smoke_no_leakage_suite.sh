#!/usr/bin/env bash
set -euo pipefail

echo "== Story 11.4 no-leakage validation suite smoke =="
echo

mkdir -p .smoke/no_leakage

echo "-- Static no-leakage contract tests"
PYTHONPATH=. pytest \
  pipelines/tests/test_no_leakage_contract.py \
  pipelines/tests/test_market_feature_export_contract.py \
  pipelines/tests/test_market_features_no_leakage.py \
  -q

echo
echo "-- Rebuild market features"
python pipelines/features/build_market_features_monthly.py

echo
echo "-- Regenerate feature exports"
python scripts/export_market_features.py --feature-version v1
python scripts/export_market_features.py --feature-version v1 --include-non-trainable

echo
echo "-- Validate feature exports"
python scripts/validate_market_feature_export.py data/exports/features/latest_v1_trainable_only.manifest.json
python scripts/validate_market_feature_export.py data/exports/features/latest_v1_all_rows.manifest.json

echo
echo "-- Runtime no-leakage validation suite"
python scripts/validate_no_leakage_suite.py

echo
echo "-- SQL no-leakage audit"
docker compose exec -T postgres psql -U onehaven -d onehaven_market < pipelines/sql/story_11_4_no_leakage_audit.sql

echo
echo "-- Machine-check SQL audit zero-bad-rows checks"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
SELECT
  (
    SELECT COUNT(*)
    FROM analytics.market_features_monthly
    WHERE is_trainable = true
      AND target_price_growth_12m IS NULL
  )::int || '|' ||
  (
    SELECT COUNT(*)
    FROM analytics.market_features_monthly
    WHERE (
        target_price_growth_12m IS NOT NULL
        OR target_rent_growth_12m IS NOT NULL
        OR target_drawdown_12m IS NOT NULL
        OR target_cycle_phase_12m IS NOT NULL
    )
    AND target_available = false
  )::int || '|' ||
  (
    SELECT COUNT(*)
    FROM analytics.market_features_monthly
    WHERE missing_feature_names::text ILIKE '%target_%'
  )::int || '|' ||
  (
    SELECT COUNT(*)
    FROM (
        SELECT geo_id, period_month, feature_version, COUNT(*) AS rows
        FROM analytics.market_features_monthly
        GROUP BY geo_id, period_month, feature_version
        HAVING COUNT(*) > 1
    ) duplicates
  )::int || '|' ||
  (
    SELECT COUNT(*)
    FROM analytics.market_features_monthly
    WHERE source_period_max > period_month
  )::int;
" > .smoke/no_leakage/bad_row_counts.txt

cat .smoke/no_leakage/bad_row_counts.txt

python - <<'PY'
from pathlib import Path

bad_counts = [
    int(part)
    for part in Path(".smoke/no_leakage/bad_row_counts.txt").read_text().strip().split("|")
]

assert bad_counts == [0, 0, 0, 0, 0], f"No-leakage bad row counts are not zero: {bad_counts}"

print("Machine-check no-leakage counts passed.")
PY

echo
echo "Story 11.4 no-leakage validation suite smoke passed."
