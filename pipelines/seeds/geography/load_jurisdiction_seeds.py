from __future__ import annotations

import argparse
import csv
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine, text


DEFAULT_PLACES_SEED_PATH = Path("data/seeds/geography/places_seed.csv")
DEFAULT_ZCTAS_SEED_PATH = Path("data/seeds/geography/zctas_seed.csv")


@dataclass(frozen=True)
class JurisdictionSeed:
    geo_id: str
    geo_type: str
    name: str
    display_name: str
    canonical_slug: str
    parent_geo_id: str | None
    state_code: str | None
    state_name: str | None
    county_fips: str | None
    cbsa_code: str | None
    place_fips: str | None
    zcta: str | None
    country_code: str
    latitude: Decimal | None
    longitude: Decimal | None
    hierarchy_level: int
    is_active: bool


def _clean(value: str | None) -> str | None:
    if value is None:
        return None

    value = value.strip()
    return value or None


def _required(row: dict[str, str], key: str) -> str:
    value = _clean(row.get(key))

    if value is None:
        raise ValueError(f"Missing required field {key!r}: {row}")

    return value


def _decimal(value: str | None) -> Decimal | None:
    cleaned = _clean(value)

    if cleaned is None:
        return None

    return Decimal(cleaned)


def _bool(value: str | None) -> bool:
    cleaned = _clean(value)

    if cleaned is None:
        return True

    return cleaned.lower() in {"true", "t", "1", "yes", "y"}


def read_seed_file(path: Path) -> list[JurisdictionSeed]:
    if not path.exists():
        return []

    seeds: list[JurisdictionSeed] = []

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            seeds.append(
                JurisdictionSeed(
                    geo_id=_required(row, "geo_id"),
                    geo_type=_required(row, "geo_type"),
                    name=_required(row, "name"),
                    display_name=_required(row, "display_name"),
                    canonical_slug=_required(row, "canonical_slug"),
                    parent_geo_id=_clean(row.get("parent_geo_id")),
                    state_code=_clean(row.get("state_code")),
                    state_name=_clean(row.get("state_name")),
                    county_fips=_clean(row.get("county_fips")),
                    cbsa_code=_clean(row.get("cbsa_code")),
                    place_fips=_clean(row.get("place_fips")),
                    zcta=_clean(row.get("zcta")),
                    country_code=_clean(row.get("country_code")) or "US",
                    latitude=_decimal(row.get("latitude")),
                    longitude=_decimal(row.get("longitude")),
                    hierarchy_level=int(_required(row, "hierarchy_level")),
                    is_active=_bool(row.get("is_active")),
                )
            )

    return seeds


def validate_parent_geo_ids(engine, seeds: list[JurisdictionSeed]) -> None:
    parent_ids = sorted({seed.parent_geo_id for seed in seeds if seed.parent_geo_id})

    if not parent_ids:
        return

    sql = text(
        """
        SELECT geo_id
        FROM geo.dim_geo
        WHERE geo_id = ANY(:geo_ids)
          AND is_active = true
        """
    )

    with engine.begin() as connection:
        found = {
            row[0]
            for row in connection.execute(sql, {"geo_ids": parent_ids}).all()
        }

    missing = sorted(set(parent_ids) - found)

    if missing:
        raise ValueError(f"parent_geo_id values not found in geo.dim_geo: {missing}")



def upsert_jurisdiction_seeds(engine, seeds: list[JurisdictionSeed]) -> int:
    if not seeds:
        return 0

    sql = text(
        """
        INSERT INTO geo.dim_geo (
            geo_id,
            geo_type,
            name,
            display_name,
            canonical_slug,
            parent_geo_id,
            state_code,
            state_name,
            county_fips,
            cbsa_code,
            place_fips,
            zcta,
            country_code,
            latitude,
            longitude,
            hierarchy_level,
            is_active
        )
        VALUES (
            :geo_id,
            :geo_type,
            :name,
            :display_name,
            :canonical_slug,
            :parent_geo_id,
            :state_code,
            :state_name,
            :county_fips,
            :cbsa_code,
            :place_fips,
            :zcta,
            :country_code,
            :latitude,
            :longitude,
            :hierarchy_level,
            :is_active
        )
        ON CONFLICT (geo_id)
        DO UPDATE SET
            geo_type = EXCLUDED.geo_type,
            name = EXCLUDED.name,
            display_name = EXCLUDED.display_name,
            canonical_slug = EXCLUDED.canonical_slug,
            parent_geo_id = EXCLUDED.parent_geo_id,
            state_code = EXCLUDED.state_code,
            state_name = EXCLUDED.state_name,
            county_fips = EXCLUDED.county_fips,
            cbsa_code = EXCLUDED.cbsa_code,
            place_fips = EXCLUDED.place_fips,
            zcta = EXCLUDED.zcta,
            country_code = EXCLUDED.country_code,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            hierarchy_level = EXCLUDED.hierarchy_level,
            is_active = EXCLUDED.is_active,
            updated_at = now()
        """
    )

    payload = [
        {
            "geo_id": seed.geo_id,
            "geo_type": seed.geo_type,
            "name": seed.name,
            "display_name": seed.display_name,
            "canonical_slug": seed.canonical_slug,
            "parent_geo_id": seed.parent_geo_id,
            "state_code": seed.state_code,
            "state_name": seed.state_name,
            "county_fips": seed.county_fips,
            "cbsa_code": seed.cbsa_code,
            "place_fips": seed.place_fips,
            "zcta": seed.zcta,
            "country_code": seed.country_code,
            "latitude": seed.latitude,
            "longitude": seed.longitude,
            "hierarchy_level": seed.hierarchy_level,
            "is_active": seed.is_active,
        }
        for seed in seeds
    ]

    with engine.begin() as connection:
        connection.execute(sql, payload)

    return len(payload)


def load_jurisdiction_seeds(
    *,
    places_seed_path: Path = DEFAULT_PLACES_SEED_PATH,
    zctas_seed_path: Path = DEFAULT_ZCTAS_SEED_PATH,
    database_url: str | None = None,
) -> int:
    place_seeds = read_seed_file(places_seed_path)
    zcta_seeds = read_seed_file(zctas_seed_path)

    resolved_database_url = database_url or os.getenv("DATABASE_URL")

    if not resolved_database_url:
        raise ValueError("DATABASE_URL is required.")

    engine = create_engine(resolved_database_url)

    loaded = 0

    # Load parents before children. ZCTAs may point to place rows loaded here.
    if place_seeds:
        validate_parent_geo_ids(engine, place_seeds)
        loaded += upsert_jurisdiction_seeds(engine, place_seeds)

    if zcta_seeds:
        validate_parent_geo_ids(engine, zcta_seeds)
        loaded += upsert_jurisdiction_seeds(engine, zcta_seeds)

    return loaded


def main() -> None:
    parser = argparse.ArgumentParser(description="Load canonical jurisdiction seed files.")
    parser.add_argument("--places-seed-path", default=str(DEFAULT_PLACES_SEED_PATH))
    parser.add_argument("--zctas-seed-path", default=str(DEFAULT_ZCTAS_SEED_PATH))

    args = parser.parse_args()

    loaded = load_jurisdiction_seeds(
        places_seed_path=Path(args.places_seed_path),
        zctas_seed_path=Path(args.zctas_seed_path),
    )

    print(f"Loaded jurisdiction seed rows: {loaded}")


if __name__ == "__main__":
    main()
