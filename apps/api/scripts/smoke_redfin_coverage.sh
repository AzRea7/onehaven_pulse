#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== v9.1 Redfin coverage smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo

mkdir -p .smoke

echo "-- Raw Redfin coverage"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
WITH stats AS (
  SELECT
    COUNT(*)::int AS raw_rows,
    COUNT(*) FILTER (
      WHERE lower(row_to_json(r)::text) LIKE '%detroit%'
    )::int AS detroit_rows
  FROM raw.redfin_market_tracker r
)
SELECT raw_rows || '|' || detroit_rows
FROM stats;
" > .smoke/redfin_raw_stats.txt

cat .smoke/redfin_raw_stats.txt

python - <<'PY'
from pathlib import Path

raw = Path(".smoke/redfin_raw_stats.txt").read_text().strip()
raw_rows, detroit_rows = [int(part) for part in raw.split("|")]

assert raw_rows > 1026, f"Still starter-scale Redfin data: raw_rows={raw_rows}"
assert detroit_rows > 0, "Detroit not found in raw.redfin_market_tracker"

print(f"Raw Redfin rows={raw_rows}, Detroit rows={detroit_rows}")
PY

echo
echo "-- Detroit analytics inventory metrics"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
SELECT
  COUNT(*) FILTER (
    WHERE active_listings IS NOT NULL
       OR months_supply IS NOT NULL
       OR median_days_on_market IS NOT NULL
  )::int
FROM analytics.market_monthly_metrics
WHERE geo_id = 'metro_19820';
" > .smoke/redfin_detroit_inventory_metric_count.txt

cat .smoke/redfin_detroit_inventory_metric_count.txt

python - <<'PY'
from pathlib import Path

count = int(Path(".smoke/redfin_detroit_inventory_metric_count.txt").read_text().strip())
assert count > 0, "metro_19820 has no transformed Redfin inventory metrics"
print(f"Detroit transformed inventory metric rows={count}")
PY

echo
echo "-- Detroit context endpoint"
curl -fsS "${API_BASE_URL}/markets/metro_19820/context" > .smoke/redfin_detroit_context.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/redfin_detroit_context.json").read_text())

assert payload["geo_id"] == "metro_19820"
assert payload["coverage"]["inventory"] is True, payload["coverage"]

confidence = payload["confidence_score"]
assert confidence >= 0.8, f"Detroit confidence is not healthy after Redfin coverage: {confidence}"

evidence = payload["evidence"]
assert (
    evidence.get("active_listings_yoy") is not None
    or evidence.get("median_days_on_market") is not None
    or evidence.get("months_supply") is not None
), evidence

print("Detroit coverage.inventory=true")
print(f"Detroit confidence_score={confidence}")
PY

echo
echo "-- Detroit coverage endpoint"
curl -fsS "${API_BASE_URL}/markets/metro_19820/coverage" > .smoke/redfin_detroit_coverage.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/redfin_detroit_coverage.json").read_text())

assert payload["geo_id"] == "metro_19820"
assert payload["coverage"]["inventory"] is True, payload["coverage"]

available = set(payload["available_metrics"])
assert {"active_listings", "median_days_on_market"} & available, available

print("Coverage endpoint confirms inventory metrics.")
PY

echo
echo "v9.1 Redfin coverage smoke passed."
