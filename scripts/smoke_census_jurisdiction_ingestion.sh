#!/usr/bin/env bash
set -euo pipefail

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market}"
export PYTHONPATH="${PYTHONPATH:-.}"

echo "== OneHaven Census jurisdiction ingestion smoke =="

python -m py_compile pipelines/sources/census/gazetteer_geographies.py
python -m py_compile pipelines/seeds/geography/load_census_jurisdictions.py

pytest pipelines/tests/test_census_jurisdiction_ingestion.py -q

python -m pipelines.seeds.geography.load_census_jurisdictions \
  --places-path data/test/census/gazetteer/places_fixture.tsv \
  --zctas-path data/test/census/gazetteer/zctas_fixture.tsv

python -m pipelines.seeds.geography.load_census_jurisdictions \
  --places-path data/test/census/gazetteer/places_fixture.tsv \
  --zctas-path data/test/census/gazetteer/zctas_fixture.tsv

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    geo_id,
    geo_type,
    display_name,
    parent_geo_id,
    state_code,
    place_fips,
    zcta,
    hierarchy_level,
    canonical_slug,
    is_active
FROM geo.dim_geo
WHERE geo_id IN ('place_2622000', 'zcta_48201', 'zcta_48202')
ORDER BY hierarchy_level, geo_id;
"

echo "Census jurisdiction ingestion smoke passed."
