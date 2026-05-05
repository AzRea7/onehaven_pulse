#!/usr/bin/env bash
set -euo pipefail

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market}"
export PYTHONPATH="${PYTHONPATH:-.}"

PLACES_PATH="${CENSUS_PLACES_LOCAL_PATH:-data/import/census/gazetteer/places.txt}"
ZCTAS_PATH="${CENSUS_ZCTAS_LOCAL_PATH:-data/import/census/gazetteer/zctas.txt}"

echo "== OneHaven Census national jurisdiction files smoke =="
echo "PLACES_PATH=${PLACES_PATH}"
echo "ZCTAS_PATH=${ZCTAS_PATH}"

if [[ ! -f "${PLACES_PATH}" || ! -f "${ZCTAS_PATH}" ]]; then
  echo "National Census jurisdiction files are missing. Skipping national ingestion smoke."
  exit 0
fi

python -m pipelines.seeds.geography.load_census_jurisdictions \
  --places-path "${PLACES_PATH}" \
  --zctas-path "${ZCTAS_PATH}"

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    geo_type,
    COUNT(*) AS rows,
    COUNT(*) FILTER (WHERE canonical_slug IS NULL OR canonical_slug = '') AS missing_slug,
    COUNT(*) FILTER (WHERE hierarchy_level IS NULL) AS missing_hierarchy_level
FROM geo.dim_geo
WHERE geo_type IN ('place', 'zcta')
GROUP BY geo_type
ORDER BY geo_type;
"

echo "Census national jurisdiction files smoke passed."
