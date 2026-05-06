#!/usr/bin/env bash
set -euo pipefail

echo "== Story 9.2 Zillow crosswalk coverage smoke =="
echo

mkdir -p .smoke

echo "-- Zillow crosswalk row count"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
SELECT COUNT(*)::int
FROM geo.geo_crosswalk
WHERE source = 'zillow'
  AND is_active = true;
" > .smoke/zillow_crosswalk_count.txt

cat .smoke/zillow_crosswalk_count.txt

python - <<'PY'
from pathlib import Path

count = int(Path(".smoke/zillow_crosswalk_count.txt").read_text().strip())
assert count > 1, f"Zillow crosswalk did not expand beyond starter mapping. count={count}"
print(f"Zillow crosswalk rows={count}")
PY

echo
echo "-- Latest Zillow transform status"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
SELECT
  status || '|' ||
  COALESCE(records_extracted, 0)::text || '|' ||
  COALESCE(records_loaded, 0)::text || '|' ||
  COALESCE(records_failed, 0)::text || '|' ||
  COALESCE(unmatched_count, 0)::text
FROM audit.pipeline_runs
WHERE source = 'zillow'
   OR pipeline_name ILIKE '%zillow%'
ORDER BY started_at DESC
LIMIT 1;
" > .smoke/zillow_latest_transform.txt

cat .smoke/zillow_latest_transform.txt

python - <<'PY'
from pathlib import Path

raw = Path(".smoke/zillow_latest_transform.txt").read_text().strip()
assert raw, "No Zillow pipeline/transform run found."

status, extracted, loaded, failed, unmatched = raw.split("|")
extracted = int(extracted)
loaded = int(loaded)
failed = int(failed)
unmatched = int(unmatched)

assert status == "success", f"Latest Zillow run did not succeed: {status}"
assert extracted > 0, "Latest Zillow run extracted no rows"
assert loaded > 0, "Latest Zillow run loaded no rows"
assert failed == 0, f"Latest Zillow run has failed rows: {failed}"

print(f"Zillow latest run status={status}, extracted={extracted}, loaded={loaded}, unmatched={unmatched}")
PY

echo
echo "-- Priority market Zillow coverage"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
WITH priority_markets AS (
    SELECT *
    FROM (
        VALUES
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
        BOOL_OR(dataset = 'zhvi') AS has_zhvi,
        BOOL_OR(dataset = 'zori') AS has_zori
    FROM analytics.market_metric_sources
    WHERE source = 'zillow'
    GROUP BY geo_id
)
SELECT
    COUNT(*) FILTER (WHERE COALESCE(has_zhvi, false))::int || '|' ||
    COUNT(*) FILTER (WHERE COALESCE(has_zori, false))::int || '|' ||
    COUNT(*)::int
FROM priority_markets p
LEFT JOIN coverage c
  ON c.geo_id = p.geo_id;
" > .smoke/zillow_priority_coverage.txt

cat .smoke/zillow_priority_coverage.txt

python - <<'PY'
from pathlib import Path

zhvi_count, zori_count, total = [
    int(part)
    for part in Path(".smoke/zillow_priority_coverage.txt").read_text().strip().split("|")
]

assert zhvi_count >= 8, f"Expected at least 8 priority metros with ZHVI, got {zhvi_count}/{total}"
assert zori_count >= 8, f"Expected at least 8 priority metros with ZORI, got {zori_count}/{total}"

print(f"Priority Zillow ZHVI coverage={zhvi_count}/{total}")
print(f"Priority Zillow ZORI coverage={zori_count}/{total}")
PY

echo
echo "-- Unmatched diagnostics exist"
test -s data/diagnostics/zillow/zillow_unmatched_regions.csv
test -s data/diagnostics/zillow/zillow_crosswalk_proposals.csv

echo "Unmatched/proposal diagnostics are present."

echo
echo "Story 9.2 Zillow crosswalk coverage smoke passed."
