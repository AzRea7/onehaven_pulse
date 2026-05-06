#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== Story 9.3 BLS LAUS coverage smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo

mkdir -p .smoke

echo "-- Latest BLS LAUS transform status"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
SELECT
  status || '|' ||
  COALESCE(records_extracted, 0)::text || '|' ||
  COALESCE(records_loaded, 0)::text || '|' ||
  COALESCE(records_failed, 0)::text || '|' ||
  COALESCE(error_message, '')
FROM audit.pipeline_runs
WHERE pipeline_name = 'bls_laus_labor_market_transform'
   OR pipeline_name = 'bls_laus_labor_market'
   OR source = 'bls_laus'
ORDER BY started_at DESC
LIMIT 1;
" > .smoke/bls_laus_latest_transform.txt

cat .smoke/bls_laus_latest_transform.txt

python - <<'PY'
from pathlib import Path

raw = Path(".smoke/bls_laus_latest_transform.txt").read_text().strip()
assert raw, "No BLS LAUS run found."

status, extracted, loaded, failed, error_message = raw.split("|", 4)

extracted = int(extracted)
loaded = int(loaded)
failed = int(failed)

assert status == "success", f"Latest BLS LAUS run failed: {status}"
assert extracted > 0, "BLS LAUS extracted no records"
assert loaded > 0, "BLS LAUS loaded no records"
assert failed == 0, f"BLS LAUS failed rows remain: {failed}. error={error_message}"

print(f"BLS LAUS latest run status={status}, extracted={extracted}, loaded={loaded}, failed={failed}")
PY

echo
echo "-- Priority metro unemployment coverage"
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
        BOOL_OR(metric_name = 'unemployment_rate') AS has_unemployment_rate
    FROM analytics.market_metric_sources
    WHERE source = 'bls_laus'
    GROUP BY geo_id
)
SELECT
    COUNT(*) FILTER (WHERE COALESCE(has_unemployment_rate, false))::int || '|' ||
    COUNT(*)::int
FROM priority_markets p
LEFT JOIN coverage c
  ON c.geo_id = p.geo_id;
" > .smoke/bls_laus_priority_coverage.txt

cat .smoke/bls_laus_priority_coverage.txt

python - <<'PY'
from pathlib import Path

covered, total = [
    int(part)
    for part in Path(".smoke/bls_laus_priority_coverage.txt").read_text().strip().split("|")
]

assert covered >= 10, f"Expected at least 10 priority metros with unemployment_rate, got {covered}/{total}"
print(f"Priority metro unemployment coverage={covered}/{total}")
PY

echo
echo "-- Context endpoints expose labor coverage"
for geo_id in metro_19820 metro_16980 metro_19100 metro_12420 metro_45300 metro_38060 metro_12060 metro_42660 metro_14460 metro_31080 metro_37980; do
  curl -fsS "${API_BASE_URL}/markets/${geo_id}/coverage" > ".smoke/bls_${geo_id}_coverage.json"

  GEO_ID="${geo_id}" python - <<'PY'
import json
import os
from pathlib import Path

geo_id = os.environ["GEO_ID"]
payload = json.loads(Path(f".smoke/bls_{geo_id}_coverage.json").read_text())

assert payload["geo_id"] == geo_id
assert payload["coverage"]["labor"] is True, payload["coverage"]

print(f"{geo_id}: labor coverage true")
PY
done

echo
echo "Story 9.3 BLS LAUS coverage smoke passed."
