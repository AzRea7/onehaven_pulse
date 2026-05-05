#!/usr/bin/env bash
set -euo pipefail

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market}"
export PYTHONPATH="${PYTHONPATH:-.}"

echo "== OneHaven geo relationship ingestion smoke =="

python -m py_compile pipelines/seeds/geography/load_geo_relationships.py
python -m py_compile pipelines/common/geography/relationships.py

pytest pipelines/tests/test_geo_relationship_loader.py -q

python -m pipelines.seeds.geography.load_geo_relationships
python -m pipelines.seeds.geography.load_geo_relationships

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    parent.geo_type AS parent_type,
    r.parent_geo_id,
    parent.display_name AS parent_name,
    child.geo_type AS child_type,
    r.child_geo_id,
    child.display_name AS child_name,
    r.relationship_type,
    r.source,
    r.confidence_score,
    r.overlap_ratio,
    r.is_active
FROM geo.geo_relationships r
JOIN geo.dim_geo parent
  ON parent.geo_id = r.parent_geo_id
JOIN geo.dim_geo child
  ON child.geo_id = r.child_geo_id
WHERE r.parent_geo_id IN ('state_26', 'metro_19820', 'place_2622000')
   OR r.child_geo_id IN ('place_2622000', 'zcta_48201', 'zcta_48202', 'zcta_48226')
ORDER BY r.parent_geo_id, r.child_geo_id;
"

pytest pipelines/tests/test_geo_relationship_queries.py -q

echo "Geo relationship ingestion smoke passed."
