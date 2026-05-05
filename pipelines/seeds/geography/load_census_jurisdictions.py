from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine, text

from pipelines.common.settings import settings
from pipelines.sources.census.gazetteer_geographies import (
    CensusPlaceRecord,
    CensusZctaRecord,
    read_census_places,
    read_census_zctas,
    slugify,
)


@dataclass(frozen=True)
class CanonicalJurisdiction:
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


def _state_parent_geo_id(state_code: str | None) -> str | None:
    if not state_code:
        return None

    return f"state_{state_code.lower()}"


def place_to_jurisdiction(record: CensusPlaceRecord) -> CanonicalJurisdiction:
    state_code = record.state_code
    state_name = record.state_name
    display_name = f"{record.name}, {state_code}" if state_code else record.name
    canonical_slug = slugify(display_name)

    return CanonicalJurisdiction(
        geo_id=record.geo_id,
        geo_type="place",
        name=record.name,
        display_name=display_name,
        canonical_slug=canonical_slug,
        parent_geo_id=_state_parent_geo_id(state_code),
        state_code=state_code,
        state_name=state_name,
        county_fips=None,
        cbsa_code=None,
        place_fips=record.full_place_fips,
        zcta=None,
        country_code="US",
        latitude=record.latitude,
        longitude=record.longitude,
        hierarchy_level=3,
        is_active=True,
    )


def zcta_to_jurisdiction(record: CensusZctaRecord) -> CanonicalJurisdiction:
    return CanonicalJurisdiction(
        geo_id=record.geo_id,
        geo_type="zcta",
        name=record.zcta,
        display_name=f"ZCTA {record.zcta}",
        canonical_slug=f"zcta-{record.zcta}",
        parent_geo_id=None,
        state_code=None,
        state_name=None,
        county_fips=None,
        cbsa_code=None,
        place_fips=None,
        zcta=record.zcta,
        country_code="US",
        latitude=record.latitude,
        longitude=record.longitude,
        hierarchy_level=4,
        is_active=True,
    )


def _existing_parent_ids(engine, parent_ids: list[str]) -> set[str]:
    if not parent_ids:
        return set()

    sql = text(
        """
        SELECT geo_id
        FROM geo.dim_geo
        WHERE geo_id = ANY(:geo_ids)
          AND is_active = true
        """
    )

    with engine.begin() as connection:
        return {
            row[0]
            for row in connection.execute(sql, {"geo_ids": parent_ids}).all()
        }


def drop_missing_parents(
    engine,
    jurisdictions: list[CanonicalJurisdiction],
) -> list[CanonicalJurisdiction]:
    parent_ids = sorted({
        item.parent_geo_id
        for item in jurisdictions
        if item.parent_geo_id
    })

    existing = _existing_parent_ids(engine, parent_ids)

    cleaned: list[CanonicalJurisdiction] = []

    for item in jurisdictions:
        if item.parent_geo_id and item.parent_geo_id not in existing:
            cleaned.append(
                CanonicalJurisdiction(
                    geo_id=item.geo_id,
                    geo_type=item.geo_type,
                    name=item.name,
                    display_name=item.display_name,
                    canonical_slug=item.canonical_slug,
                    parent_geo_id=None,
                    state_code=item.state_code,
                    state_name=item.state_name,
                    county_fips=item.county_fips,
                    cbsa_code=item.cbsa_code,
                    place_fips=item.place_fips,
                    zcta=item.zcta,
                    country_code=item.country_code,
                    latitude=item.latitude,
                    longitude=item.longitude,
                    hierarchy_level=item.hierarchy_level,
                    is_active=item.is_active,
                )
            )
        else:
            cleaned.append(item)

    return cleaned


def upsert_jurisdictions(engine, jurisdictions: list[CanonicalJurisdiction]) -> int:
    if not jurisdictions:
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
            "geo_id": item.geo_id,
            "geo_type": item.geo_type,
            "name": item.name,
            "display_name": item.display_name,
            "canonical_slug": item.canonical_slug,
            "parent_geo_id": item.parent_geo_id,
            "state_code": item.state_code,
            "state_name": item.state_name,
            "county_fips": item.county_fips,
            "cbsa_code": item.cbsa_code,
            "place_fips": item.place_fips,
            "zcta": item.zcta,
            "country_code": item.country_code,
            "latitude": item.latitude,
            "longitude": item.longitude,
            "hierarchy_level": item.hierarchy_level,
            "is_active": item.is_active,
        }
        for item in jurisdictions
    ]

    with engine.begin() as connection:
        connection.execute(sql, payload)

    return len(payload)


def load_census_jurisdictions(
    *,
    places_path: Path | None = None,
    zctas_path: Path | None = None,
    database_url: str | None = None,
) -> int:
    resolved_places_path = places_path or Path(settings.census_places_local_path)
    resolved_zctas_path = zctas_path or Path(settings.census_zctas_local_path)
    resolved_database_url = database_url or os.getenv("DATABASE_URL") or settings.database_url

    engine = create_engine(resolved_database_url)

    place_records = read_census_places(resolved_places_path)
    zcta_records = read_census_zctas(resolved_zctas_path)

    jurisdictions = [
        *[place_to_jurisdiction(record) for record in place_records],
        *[zcta_to_jurisdiction(record) for record in zcta_records],
    ]

    jurisdictions = drop_missing_parents(engine, jurisdictions)

    return upsert_jurisdictions(engine, jurisdictions)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Census Gazetteer place/ZCTA jurisdictions.")
    parser.add_argument("--places-path", default=None)
    parser.add_argument("--zctas-path", default=None)

    args = parser.parse_args()

    loaded = load_census_jurisdictions(
        places_path=Path(args.places_path) if args.places_path else None,
        zctas_path=Path(args.zctas_path) if args.zctas_path else None,
    )

    print(f"Loaded Census jurisdiction rows: {loaded}")


if __name__ == "__main__":
    main()
