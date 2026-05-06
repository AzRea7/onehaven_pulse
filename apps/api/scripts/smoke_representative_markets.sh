#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== Story 9.5 representative market validation smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo

mkdir -p .smoke
mkdir -p data/diagnostics/representative_markets

echo "-- Docker services"
docker compose ps

echo
echo "-- Representative geographies exist"
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
)
SELECT
    COUNT(*) FILTER (WHERE g.geo_id IS NOT NULL)::int || '|' ||
    COUNT(*)::int
FROM representative_markets r
LEFT JOIN geo.dim_geo g
  ON g.geo_id = r.geo_id;
" > .smoke/representative_geo_count.txt

cat .smoke/representative_geo_count.txt

python - <<'PY'
from pathlib import Path

found, total = [
    int(part)
    for part in Path(".smoke/representative_geo_count.txt").read_text().strip().split("|")
]

assert found == total, f"Representative geographies missing: found={found}, total={total}"
print(f"Representative geographies exist={found}/{total}")
PY

echo
echo "-- Representative market DB coverage sanity"
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
        BOOL_OR(zhvi IS NOT NULL OR zhvi_yoy IS NOT NULL) AS has_price,
        BOOL_OR(zori IS NOT NULL OR zori_yoy IS NOT NULL) AS has_rent,
        BOOL_OR(payment_to_income_ratio IS NOT NULL OR price_to_income_ratio IS NOT NULL) AS has_affordability,
        BOOL_OR(unemployment_rate IS NOT NULL) AS has_labor
    FROM analytics.market_monthly_metrics
    WHERE geo_id IN (SELECT geo_id FROM representative_markets)
    GROUP BY geo_id
)
SELECT
    COUNT(*) FILTER (WHERE COALESCE(has_price, false))::int || '|' ||
    COUNT(*) FILTER (WHERE COALESCE(has_rent, false))::int || '|' ||
    COUNT(*) FILTER (WHERE COALESCE(has_affordability, false))::int || '|' ||
    COUNT(*) FILTER (WHERE COALESCE(has_labor, false))::int || '|' ||
    COUNT(*)::int
FROM representative_markets r
LEFT JOIN coverage c
  ON c.geo_id = r.geo_id;
" > .smoke/representative_db_coverage.txt

cat .smoke/representative_db_coverage.txt

python - <<'PY'
from pathlib import Path

price, rent, affordability, labor, total = [
    int(part)
    for part in Path(".smoke/representative_db_coverage.txt").read_text().strip().split("|")
]

assert price >= 11, f"Expected at least 11 representative markets with price coverage, got {price}/{total}"
assert rent >= 11, f"Expected at least 11 representative markets with rent coverage, got {rent}/{total}"
assert affordability >= 11, f"Expected at least 11 representative markets with affordability coverage, got {affordability}/{total}"
assert labor >= 11, f"Expected at least 11 representative markets with labor coverage, got {labor}/{total}"

print(f"DB representative coverage price={price}/{total}, rent={rent}/{total}, affordability={affordability}/{total}, labor={labor}/{total}")
PY

echo
echo "-- API representative market validation"
API_BASE_URL="${API_BASE_URL}" python scripts/validate_representative_markets.py

cp data/diagnostics/representative_markets/story_9_5_representative_market_validation.json \
  .smoke/representative_market_validation.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/representative_market_validation.json").read_text())

assert payload["market_count"] == 12, payload
assert payload["context_endpoint_ok_count"] == 12, payload
assert payload["coverage_endpoint_ok_count"] == 12, payload
assert payload["confidence_ge_0_8_count"] >= 8, payload
assert payload["passes_story_9_5"] is True, payload

print("Representative API validation passed.")
PY

echo
echo "-- Missing-data explanations are present when coverage is incomplete"
python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path("data/diagnostics/representative_markets/story_9_5_representative_market_validation.json").read_text())

bad = []

for result in payload["results"]:
    coverage = result.get("coverage") or {}
    has_false_category = any(value is False for value in coverage.values())
    explanations = result.get("missing_data_explanations") or []

    if has_false_category and not explanations:
        bad.append(result["geo_id"])

assert not bad, f"Markets have incomplete coverage but no explanations: {bad}"

print("Missing-data states are explainable.")
PY

echo
echo "Story 9.5 representative market validation smoke passed."
