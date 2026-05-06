#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== Story 9.4 affordability coverage smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo

mkdir -p .smoke

echo "-- Required affordability columns exist"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
SELECT
  COUNT(*) FILTER (WHERE column_name = 'price_to_income_ratio')::int || '|' ||
  COUNT(*) FILTER (WHERE column_name = 'estimated_monthly_payment')::int || '|' ||
  COUNT(*) FILTER (WHERE column_name = 'payment_to_income_ratio')::int
FROM information_schema.columns
WHERE table_schema = 'analytics'
  AND table_name = 'market_monthly_metrics';
" > .smoke/affordability_columns.txt

cat .smoke/affordability_columns.txt

python - <<'PY'
from pathlib import Path

price_to_income, estimated_payment, payment_to_income = [
    int(part)
    for part in Path(".smoke/affordability_columns.txt").read_text().strip().split("|")
]

assert price_to_income == 1, "price_to_income_ratio column missing"
assert estimated_payment == 1, "estimated_monthly_payment column missing"
assert payment_to_income == 1, "payment_to_income_ratio column missing"

print("Required affordability columns exist.")
PY

echo
echo "-- Priority affordability metric coverage"
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
        BOOL_OR(price_to_income_ratio IS NOT NULL) AS has_price_to_income,
        BOOL_OR(estimated_monthly_payment IS NOT NULL) AS has_estimated_payment,
        BOOL_OR(payment_to_income_ratio IS NOT NULL) AS has_payment_to_income
    FROM analytics.market_monthly_metrics
    GROUP BY geo_id
)
SELECT
    COUNT(*) FILTER (WHERE COALESCE(has_price_to_income, false))::int || '|' ||
    COUNT(*) FILTER (WHERE COALESCE(has_estimated_payment, false))::int || '|' ||
    COUNT(*) FILTER (WHERE COALESCE(has_payment_to_income, false))::int || '|' ||
    COUNT(*)::int
FROM priority_markets p
LEFT JOIN coverage c
  ON c.geo_id = p.geo_id;
" > .smoke/affordability_priority_coverage.txt

cat .smoke/affordability_priority_coverage.txt

python - <<'PY'
from pathlib import Path

price_to_income, estimated_payment, payment_to_income, total = [
    int(part)
    for part in Path(".smoke/affordability_priority_coverage.txt").read_text().strip().split("|")
]

assert price_to_income >= 8, f"Expected >=8 priority metros with price_to_income_ratio, got {price_to_income}/{total}"
assert estimated_payment >= 8, f"Expected >=8 priority metros with estimated_monthly_payment, got {estimated_payment}/{total}"
assert payment_to_income >= 8, f"Expected >=8 priority metros with payment_to_income_ratio, got {payment_to_income}/{total}"

print(f"Priority price_to_income coverage={price_to_income}/{total}")
print(f"Priority estimated_payment coverage={estimated_payment}/{total}")
print(f"Priority payment_to_income coverage={payment_to_income}/{total}")
PY

echo
echo "-- Source flags preserve derivation"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
SELECT COUNT(*)::int
FROM analytics.market_monthly_metrics
WHERE payment_to_income_ratio IS NOT NULL
  AND source_flags::jsonb ? 'payment_to_income_ratio';
" > .smoke/affordability_source_flags.txt

cat .smoke/affordability_source_flags.txt

python - <<'PY'
from pathlib import Path

count = int(Path(".smoke/affordability_source_flags.txt").read_text().strip())
assert count > 0, "No payment_to_income_ratio source flags found"
print(f"Affordability source flag rows={count}")
PY

echo
echo "-- No affordability metric has impossible values"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
SELECT COUNT(*)::int
FROM analytics.market_monthly_metrics
WHERE price_to_income_ratio < 0
   OR estimated_monthly_payment < 0
   OR payment_to_income_ratio < 0
   OR payment_to_income_ratio > 5;
" > .smoke/affordability_bad_values.txt

cat .smoke/affordability_bad_values.txt

python - <<'PY'
from pathlib import Path

bad = int(Path(".smoke/affordability_bad_values.txt").read_text().strip())
assert bad == 0, f"Found impossible affordability values: {bad}"
print("No impossible affordability values found.")
PY

echo
echo "-- API coverage exposes affordability"
for geo_id in metro_19820 metro_16980 metro_19100 metro_12420 metro_45300 metro_38060 metro_12060 metro_42660 metro_14460 metro_31080 metro_37980; do
  curl -fsS "${API_BASE_URL}/markets/${geo_id}/coverage" > ".smoke/affordability_${geo_id}_coverage.json"

  GEO_ID="${geo_id}" python - <<'PY'
import json
import os
from pathlib import Path

geo_id = os.environ["GEO_ID"]
payload = json.loads(Path(f".smoke/affordability_{geo_id}_coverage.json").read_text())

assert payload["geo_id"] == geo_id
assert payload["coverage"]["affordability"] is True, payload["coverage"]

print(f"{geo_id}: affordability coverage true")
PY
done

echo
echo "Story 9.4 affordability coverage smoke passed."
