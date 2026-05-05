#!/usr/bin/env bash
set -euo pipefail

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market}"
export PYTHONPATH="${PYTHONPATH:-.}"

echo "== OneHaven jurisdiction seed ingestion smoke =="

python -m py_compile pipelines/seeds/geography/load_jurisdiction_seeds.py
python -m py_compile pipelines/common/geography/resolver.py

python -m pipelines.seeds.geography.load_jurisdiction_seeds
python -m pipelines.seeds.geography.load_jurisdiction_seeds

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
WHERE geo_id IN ('place_2622000', 'zcta_48201', 'zcta_48202', 'zcta_48226')
ORDER BY geo_type, geo_id;
"

PYTHONPATH=. python - <<'PYCODE'
from sqlalchemy import create_engine

from pipelines.common.settings import settings
from pipelines.common.geography.resolver import GeographyResolver

engine = create_engine(settings.database_url)

with engine.begin() as connection:
    resolver = GeographyResolver(connection)

    place = resolver.resolve(place_fips="2622000")
    zcta = resolver.resolve(zcta="48201")

    print("place result:", place)
    print("zcta result:", zcta)

    if place is None or place.canonical_geo_id != "place_2622000":
        raise SystemExit("place_fips resolver failed")

    if zcta is None or zcta.canonical_geo_id != "zcta_48201":
        raise SystemExit("zcta resolver failed")

print("Jurisdiction seed ingestion smoke passed.")
PYCODE
