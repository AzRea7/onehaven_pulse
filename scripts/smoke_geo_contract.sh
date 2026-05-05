#!/usr/bin/env bash
set -euo pipefail

echo "== OneHaven geography contract smoke =="

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'geo'
  AND table_name = 'dim_geo'
  AND column_name IN (
    'geo_id',
    'geo_type',
    'name',
    'display_name',
    'state_code',
    'state_name',
    'county_fips',
    'cbsa_code',
    'place_fips',
    'zcta',
    'country_code',
    'latitude',
    'longitude',
    'parent_geo_id',
    'hierarchy_level',
    'canonical_slug',
    'is_active',
    'created_at',
    'updated_at'
  )
ORDER BY column_name;
"

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    geo_id,
    geo_type,
    display_name,
    cbsa_code,
    place_fips,
    hierarchy_level,
    canonical_slug
FROM geo.dim_geo
WHERE geo_id IN ('us', 'metro_19820')
ORDER BY geo_id;
"

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    geo_type,
    COUNT(*) AS rows,
    COUNT(*) FILTER (WHERE canonical_slug IS NULL) AS missing_slug,
    COUNT(*) FILTER (WHERE hierarchy_level IS NULL) AS missing_hierarchy_level
FROM geo.dim_geo
GROUP BY geo_type
ORDER BY geo_type;
"

echo "Geography contract smoke passed."
