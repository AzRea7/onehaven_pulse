#!/usr/bin/env bash
set -euo pipefail

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market}"

echo "== OneHaven geography resolver smoke =="
echo "DATABASE_URL=${DATABASE_URL}"
echo

PYTHONPATH=. python - <<'PYCODE'
from sqlalchemy import create_engine

from pipelines.common.geography.resolver import GeographyResolver

engine = create_engine(__import__("os").environ["DATABASE_URL"])

with engine.begin() as connection:
    resolver = GeographyResolver(connection)

    checks = [
        (
            "zillow Detroit crosswalk",
            {
                "source": "zillow",
                "source_geo_id": "394532",
            },
            "metro_19820",
        ),
        (
            "BLS Detroit LAUS unemployment crosswalk",
            {
                "source": "bls_laus",
                "source_geo_id": "LAUMT261982000000003",
            },
            "metro_19820",
        ),
        (
            "direct canonical geo_id",
            {
                "source_geo_id": "metro_19820",
            },
            "metro_19820",
        ),
        (
            "CBSA fallback",
            {
                "cbsa_code": "19820",
            },
            "metro_19820",
        ),
    ]

    for name, kwargs, expected in checks:
        result = resolver.resolve(**kwargs)

        if result is None:
            raise SystemExit(f"{name}: expected {expected}, got None")

        print(name)
        print("  canonical_geo_id =", result.canonical_geo_id)
        print("  match_method     =", result.match_method)
        print("  confidence       =", result.confidence_score)

        if result.canonical_geo_id != expected:
            raise SystemExit(
                f"{name}: expected {expected}, got {result.canonical_geo_id}"
            )

    missing = resolver.resolve(source="not_a_source", source_geo_id="not_real")
    print("missing lookup")
    print("  result =", missing)

    if missing is not None:
        raise SystemExit("missing lookup should return None")

print("Geography resolver smoke passed.")
PYCODE
