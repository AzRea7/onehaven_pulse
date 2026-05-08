#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== Michigan data freshness smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo

mkdir -p .smoke/michigan_data data/diagnostics/michigan

echo "-- Michigan jurisdictions exist in geo.dim_geo"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
WITH mi AS (
    SELECT
        geo_id,
        geo_type,
        COALESCE(display_name, name) AS market_name,
        state_code
    FROM geo.dim_geo
    WHERE is_active = true
      AND (
            upper(COALESCE(state_code, '')) = 'MI'
            OR upper(COALESCE(name, '')) ~ '(^|[^A-Z])MI($|[^A-Z])'
            OR upper(COALESCE(display_name, '')) ~ '(^|[^A-Z])MI($|[^A-Z])'
      )
)
SELECT
    COUNT(*) AS mi_jurisdiction_count
FROM mi;
" > .smoke/michigan_data/mi_jurisdiction_count.txt

cat .smoke/michigan_data/mi_jurisdiction_count.txt

python - <<'PY'
from pathlib import Path

count = int(Path(".smoke/michigan_data/mi_jurisdiction_count.txt").read_text().strip())
assert count > 0, "Expected Michigan jurisdictions in geo.dim_geo"
print(f"Michigan jurisdiction count passed: {count}")
PY

echo
echo "-- Detroit metro exists"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
SELECT COUNT(*)
FROM geo.dim_geo
WHERE geo_id = 'metro_19820'
  AND is_active = true;
" > .smoke/michigan_data/detroit_exists.txt

cat .smoke/michigan_data/detroit_exists.txt

python - <<'PY'
from pathlib import Path

count = int(Path(".smoke/michigan_data/detroit_exists.txt").read_text().strip())
assert count == 1, "Expected Detroit metro metro_19820 to exist"
print("Detroit metro exists.")
PY

echo
echo "-- Michigan latest metric freshness by family"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
WITH mi_geos AS (
    SELECT geo_id
    FROM geo.dim_geo
    WHERE is_active = true
      AND (
            upper(COALESCE(state_code, '')) = 'MI'
            OR upper(COALESCE(name, '')) ~ '(^|[^A-Z])MI($|[^A-Z])'
            OR upper(COALESCE(display_name, '')) ~ '(^|[^A-Z])MI($|[^A-Z])'
      )
),
families AS (
    SELECT
        'price' AS family,
        MAX(period_month) FILTER (
            WHERE zhvi_yoy IS NOT NULL
               OR home_price_index_yoy IS NOT NULL
               OR median_sale_price_yoy IS NOT NULL
        ) AS latest_period
    FROM analytics.market_monthly_metrics
    WHERE geo_id IN (SELECT geo_id FROM mi_geos)

    UNION ALL
    SELECT
        'rent',
        MAX(period_month) FILTER (
            WHERE zori_yoy IS NOT NULL
               OR median_rent_yoy IS NOT NULL
        )
    FROM analytics.market_monthly_metrics
    WHERE geo_id IN (SELECT geo_id FROM mi_geos)

    UNION ALL
    SELECT
        'inventory',
        MAX(period_month) FILTER (
            WHERE active_listings_yoy IS NOT NULL
               OR months_supply IS NOT NULL
               OR median_days_on_market IS NOT NULL
        )
    FROM analytics.market_monthly_metrics
    WHERE geo_id IN (SELECT geo_id FROM mi_geos)

    UNION ALL
    SELECT
        'affordability',
        MAX(period_month) FILTER (
            WHERE payment_to_income_ratio IS NOT NULL
               OR price_to_income_ratio IS NOT NULL
               OR estimated_monthly_payment IS NOT NULL
        )
    FROM analytics.market_monthly_metrics
    WHERE geo_id IN (SELECT geo_id FROM mi_geos)

    UNION ALL
    SELECT
        'labor',
        MAX(period_month) FILTER (
            WHERE unemployment_rate IS NOT NULL
        )
    FROM analytics.market_monthly_metrics
    WHERE geo_id IN (SELECT geo_id FROM mi_geos)

    UNION ALL
    SELECT
        'permits',
        MAX(period_month) FILTER (
            WHERE building_permits IS NOT NULL
               OR permits_per_1000_people IS NOT NULL
        )
    FROM analytics.market_monthly_metrics
    WHERE geo_id IN (SELECT geo_id FROM mi_geos)
)
SELECT
    family,
    latest_period,
    CASE
        WHEN latest_period IS NULL THEN NULL
        ELSE CURRENT_DATE - latest_period
    END AS age_days,
    CASE
        WHEN latest_period IS NULL THEN 'missing'
        WHEN family = 'permits' AND CURRENT_DATE - latest_period <= 120 THEN 'fresh'
        WHEN family <> 'permits' AND CURRENT_DATE - latest_period <= 90 THEN 'fresh'
        ELSE 'stale'
    END AS freshness_status
FROM families
ORDER BY family;
" | tee data/diagnostics/michigan/latest_metric_freshness.txt

echo
echo "-- Machine-check required Michigan freshness"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
WITH mi_geos AS (
    SELECT geo_id
    FROM geo.dim_geo
    WHERE is_active = true
      AND (
            upper(COALESCE(state_code, '')) = 'MI'
            OR upper(COALESCE(name, '')) ~ '(^|[^A-Z])MI($|[^A-Z])'
            OR upper(COALESCE(display_name, '')) ~ '(^|[^A-Z])MI($|[^A-Z])'
      )
),
families AS (
    SELECT
        'price' AS family,
        MAX(period_month) FILTER (
            WHERE zhvi_yoy IS NOT NULL
               OR home_price_index_yoy IS NOT NULL
               OR median_sale_price_yoy IS NOT NULL
        ) AS latest_period
    FROM analytics.market_monthly_metrics
    WHERE geo_id IN (SELECT geo_id FROM mi_geos)

    UNION ALL
    SELECT
        'rent',
        MAX(period_month) FILTER (
            WHERE zori_yoy IS NOT NULL
               OR median_rent_yoy IS NOT NULL
        )
    FROM analytics.market_monthly_metrics
    WHERE geo_id IN (SELECT geo_id FROM mi_geos)

    UNION ALL
    SELECT
        'affordability',
        MAX(period_month) FILTER (
            WHERE payment_to_income_ratio IS NOT NULL
               OR price_to_income_ratio IS NOT NULL
               OR estimated_monthly_payment IS NOT NULL
        )
    FROM analytics.market_monthly_metrics
    WHERE geo_id IN (SELECT geo_id FROM mi_geos)

    UNION ALL
    SELECT
        'labor',
        MAX(period_month) FILTER (
            WHERE unemployment_rate IS NOT NULL
        )
    FROM analytics.market_monthly_metrics
    WHERE geo_id IN (SELECT geo_id FROM mi_geos)
),
bad AS (
    SELECT *
    FROM families
    WHERE latest_period IS NULL
       OR CURRENT_DATE - latest_period > 90
)
SELECT COUNT(*) FROM bad;
" > .smoke/michigan_data/bad_required_freshness_count.txt

cat .smoke/michigan_data/bad_required_freshness_count.txt

python - <<'PY'
from pathlib import Path

bad = int(Path(".smoke/michigan_data/bad_required_freshness_count.txt").read_text().strip())
assert bad == 0, f"Michigan required data families are missing/stale: bad_count={bad}"
print("Michigan required freshness passed.")
PY

echo
echo "-- Michigan bad value check"
docker compose exec -T postgres psql -U onehaven -d onehaven_market -Atc "
WITH mi_geos AS (
    SELECT geo_id
    FROM geo.dim_geo
    WHERE is_active = true
      AND (
            upper(COALESCE(state_code, '')) = 'MI'
            OR upper(COALESCE(name, '')) ~ '(^|[^A-Z])MI($|[^A-Z])'
            OR upper(COALESCE(display_name, '')) ~ '(^|[^A-Z])MI($|[^A-Z])'
      )
)
SELECT COUNT(*)
FROM analytics.market_monthly_metrics
WHERE geo_id IN (SELECT geo_id FROM mi_geos)
  AND (
        payment_to_income_ratio < 0
     OR payment_to_income_ratio > 1.5
     OR unemployment_rate < 0
     OR unemployment_rate > 40
     OR zhvi <= 0
     OR zori <= 0
     OR median_days_on_market < 0
     OR median_days_on_market > 730
  );
" > .smoke/michigan_data/bad_values_count.txt

cat .smoke/michigan_data/bad_values_count.txt

python - <<'PY'
from pathlib import Path

bad = int(Path(".smoke/michigan_data/bad_values_count.txt").read_text().strip())
assert bad == 0, f"Michigan has impossible metric values: bad_count={bad}"
print("Michigan bad value check passed.")
PY

echo
echo "-- Michigan map boundary payload includes Detroit"
curl -fsS "${API_BASE_URL}/map/markets?geo_type=metro&metric=composite_cycle_score&state=MI" \
  > .smoke/michigan_data/map_mi.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/michigan_data/map_mi.json").read_text())
features = payload["features"]

detroit = [
    feature for feature in features
    if feature["properties"]["geo_id"] == "metro_19820"
    or "detroit" in (
        feature["properties"].get("display_name")
        or feature["properties"].get("name")
        or ""
    ).lower()
]

print(f"MI map features={len(features)}")
print(
    "Detroit matches=",
    [
        feature["properties"].get("display_name")
        or feature["properties"].get("name")
        for feature in detroit
    ],
)

assert payload["type"] == "FeatureCollection"
assert len(features) > 0
assert any(feature["geometry"] for feature in features), "Expected Michigan boundary geometry"
assert detroit, "Detroit metro should appear in MI map scope"

print("Michigan map boundary payload passed.")
PY

echo
echo "-- Michigan screener includes Detroit"
curl -fsS "${API_BASE_URL}/markets/screener?geo_type=metro&state=MI&limit=100" \
  > .smoke/michigan_data/screener_mi.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/michigan_data/screener_mi.json").read_text())
items = payload["items"]

detroit = [
    item for item in items
    if item["market"]["geo_id"] == "metro_19820"
    or "detroit" in (
        item["market"].get("display_name")
        or item["market"].get("name")
        or ""
    ).lower()
]

print(f"MI screener items={len(items)} total={payload['total']}")
print(
    "Detroit matches=",
    [
        item["market"].get("display_name")
        or item["market"].get("name")
        for item in detroit
    ],
)

assert detroit, "Detroit metro should appear in MI screener scope"

print("Michigan screener payload passed.")
PY

echo
echo "-- Detroit investor signal"
curl -fsS "${API_BASE_URL}/markets/metro_19820/investor-signal" \
  > .smoke/michigan_data/detroit_signal.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/michigan_data/detroit_signal.json").read_text())

assert payload["geo_id"] == "metro_19820"
assert payload["rule_version"] == "investor_signal_v2"
assert payload["stance"] in {
    "attractive",
    "watchlist",
    "mixed",
    "avoid",
    "insufficient_data",
}

print(
    "Detroit signal:",
    payload["stance"],
    "score=",
    payload["stance_score"],
    "missing=",
    payload["missing_score_inputs"],
)
PY

echo
echo "-- Optional market data quality check"
if curl -fsS "${API_BASE_URL}/data-quality/markets/metro_19820" \
  > .smoke/michigan_data/detroit_quality.json; then
  python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/michigan_data/detroit_quality.json").read_text())

assert payload["geo_id"] == "metro_19820"
assert 0 <= payload["overall_quality_score"] <= 1

print(
    "Detroit quality:",
    payload["overall_quality_score"],
    "coverage=",
    payload["coverage_score"],
    "freshness=",
    payload["freshness_score"],
    "missing=",
    payload["missing_categories"],
)
PY
else
  echo "Data quality endpoint not available yet; skipping."
fi

echo
echo "Michigan data freshness smoke passed."
