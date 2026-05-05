#!/usr/bin/env bash
set -euo pipefail

echo "== OneHaven source geography crosswalk seed smoke =="

DATABASE_URL="${DATABASE_URL:-postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market}" \
PYTHONPATH=. \
python -m pipelines.seeds.geography.load_source_geo_crosswalk

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    source,
    source_geo_id,
    source_geo_name,
    source_geo_type,
    canonical_geo_id,
    match_method,
    confidence_score
FROM geo.geo_crosswalk
WHERE canonical_geo_id = 'metro_19820'
ORDER BY source, source_geo_id;
"

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    source,
    source_geo_id,
    canonical_geo_id,
    COUNT(*) AS rows
FROM geo.geo_crosswalk
GROUP BY source, source_geo_id, canonical_geo_id
HAVING COUNT(*) > 1;
"

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    source,
    COUNT(*) AS rows,
    COUNT(DISTINCT canonical_geo_id) AS canonical_geo_count
FROM geo.geo_crosswalk
GROUP BY source
ORDER BY source;
"

echo "Source geography crosswalk seed smoke passed."
