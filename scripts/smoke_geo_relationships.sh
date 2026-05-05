#!/usr/bin/env bash
set -euo pipefail

echo "== OneHaven geography relationships smoke =="

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    relationship_type,
    source,
    COUNT(*) AS rows
FROM geo.geo_relationship
WHERE is_active = true
GROUP BY relationship_type, source
ORDER BY relationship_type, source;
"

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    parent_geo_id,
    child_geo_id,
    relationship_type,
    source,
    confidence_score
FROM geo.geo_relationship
WHERE parent_geo_id = 'us'
  AND child_geo_id IN ('state_mi', 'metro_19820')
  AND is_active = true
ORDER BY child_geo_id, relationship_type;
"

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    parent_geo_id,
    child_geo_id,
    relationship_type,
    source,
    confidence_score
FROM geo.geo_relationship
WHERE parent_geo_id = 'state_mi'
  AND child_geo_id LIKE 'county_%'
  AND is_active = true
ORDER BY child_geo_id
LIMIT 20;
"

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    parent_geo_id,
    child_geo_id,
    relationship_type,
    source,
    COUNT(*) AS duplicates
FROM geo.geo_relationship
WHERE is_active = true
GROUP BY parent_geo_id, child_geo_id, relationship_type, source
HAVING COUNT(*) > 1;
"

echo "Geography relationships smoke passed."
