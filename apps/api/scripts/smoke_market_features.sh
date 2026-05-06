#!/usr/bin/env bash
set -euo pipefail

echo "== Story 11.1 market features smoke =="
echo

mkdir -p .smoke/features

echo "-- Static no-leakage tests"
PYTHONPATH=. pytest pipelines/tests/test_market_features_no_leakage.py -q

echo
echo "-- Build market features"
python pipelines/features/build_market_features_monthly.py

echo
echo "-- Feature table exists and has rows"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
SELECT
  COUNT(*)::int || '|' ||
  COUNT(DISTINCT geo_id)::int || '|' ||
  COUNT(*) FILTER (WHERE is_trainable)::int || '|' ||
  COUNT(*) FILTER (WHERE target_available)::int
FROM analytics.market_features_monthly
WHERE feature_version = 'v1';
" > .smoke/features/feature_counts.txt

cat .smoke/features/feature_counts.txt

python - <<'PY'
from pathlib import Path

rows, geos, trainable, target_available = [
    int(part)
    for part in Path(".smoke/features/feature_counts.txt").read_text().strip().split("|")
]

assert rows > 1000, f"Expected >1000 feature rows, got {rows}"
assert geos >= 50, f"Expected at least 50 geos, got {geos}"
assert target_available > 0, "Expected some target_available rows"
assert trainable > 0, "Expected some trainable rows"

print(f"Feature rows={rows}, geos={geos}, trainable={trainable}, target_available={target_available}")
PY

echo
echo "-- Representative market coverage"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
WITH representative_markets AS (
    SELECT *
    FROM (
        VALUES
            ('us'),
            ('metro_19820'),
            ('metro_16980'),
            ('metro_19100'),
            ('metro_12420'),
            ('metro_45300'),
            ('metro_38060'),
            ('metro_12060'),
            ('metro_42660'),
            ('metro_14460'),
            ('metro_31080'),
            ('metro_37980')
    ) AS t(geo_id)
),
coverage AS (
    SELECT
        geo_id,
        COUNT(*) AS rows
    FROM analytics.market_features_monthly
    WHERE geo_id IN (SELECT geo_id FROM representative_markets)
      AND feature_version = 'v1'
    GROUP BY geo_id
)
SELECT
  COUNT(*) FILTER (WHERE COALESCE(c.rows, 0) > 0)::int || '|' ||
  COUNT(*)::int
FROM representative_markets r
LEFT JOIN coverage c
  ON c.geo_id = r.geo_id;
" > .smoke/features/representative_coverage.txt

cat .smoke/features/representative_coverage.txt

python - <<'PY'
from pathlib import Path

covered, total = [
    int(part)
    for part in Path(".smoke/features/representative_coverage.txt").read_text().strip().split("|")
]

assert covered >= 11, f"Expected at least 11 representative markets with feature rows, got {covered}/{total}"
print(f"Representative feature coverage={covered}/{total}")
PY

echo
echo "-- No duplicate keys"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
SELECT COUNT(*)::int
FROM (
    SELECT geo_id, period_month, feature_version, COUNT(*) AS rows
    FROM analytics.market_features_monthly
    GROUP BY geo_id, period_month, feature_version
    HAVING COUNT(*) > 1
) duplicates;
" > .smoke/features/duplicate_count.txt

cat .smoke/features/duplicate_count.txt

python - <<'PY'
from pathlib import Path

duplicates = int(Path(".smoke/features/duplicate_count.txt").read_text().strip())
assert duplicates == 0, f"Duplicate feature keys found: {duplicates}"
print("No duplicate feature keys.")
PY

echo
echo "-- No-leakage validation"
python scripts/validate_market_features_no_leakage.py

echo
echo "-- Audit output"
docker compose exec -T postgres psql -U onehaven -d onehaven_market < pipelines/sql/story_11_1_market_features_audit.sql

echo
echo "Story 11.1 market features smoke passed."
