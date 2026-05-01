import json
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text

from pipelines.common.db import engine


UPSERT_CENSUS_ACS_PROFILE_SQL = text(
    """
    INSERT INTO raw.census_acs_profile (
        geography_level,
        source_geo_id,
        source_name,
        state_fips,
        county_fips,
        cbsa_code,
        year,
        source_period_start,
        source_period_end,
        total_population,
        median_household_income,
        total_housing_units,
        occupied_housing_units,
        vacant_housing_units,
        owner_occupied_housing_units,
        renter_occupied_housing_units,
        median_gross_rent,
        rent_burden_pct,
        source_payload,
        source_file_id,
        load_date
    )
    VALUES (
        :geography_level,
        :source_geo_id,
        :source_name,
        :state_fips,
        :county_fips,
        :cbsa_code,
        :year,
        :source_period_start,
        :source_period_end,
        :total_population,
        :median_household_income,
        :total_housing_units,
        :occupied_housing_units,
        :vacant_housing_units,
        :owner_occupied_housing_units,
        :renter_occupied_housing_units,
        :median_gross_rent,
        :rent_burden_pct,
        CAST(:source_payload AS JSONB),
        :source_file_id,
        :load_date
    )
    ON CONFLICT (
        geography_level,
        source_geo_id,
        year,
        load_date
    )
    DO UPDATE SET
        source_name = EXCLUDED.source_name,
        state_fips = EXCLUDED.state_fips,
        county_fips = EXCLUDED.county_fips,
        cbsa_code = EXCLUDED.cbsa_code,
        source_period_start = EXCLUDED.source_period_start,
        source_period_end = EXCLUDED.source_period_end,
        total_population = EXCLUDED.total_population,
        median_household_income = EXCLUDED.median_household_income,
        total_housing_units = EXCLUDED.total_housing_units,
        occupied_housing_units = EXCLUDED.occupied_housing_units,
        vacant_housing_units = EXCLUDED.vacant_housing_units,
        owner_occupied_housing_units = EXCLUDED.owner_occupied_housing_units,
        renter_occupied_housing_units = EXCLUDED.renter_occupied_housing_units,
        median_gross_rent = EXCLUDED.median_gross_rent,
        rent_burden_pct = EXCLUDED.rent_burden_pct,
        source_payload = EXCLUDED.source_payload,
        source_file_id = EXCLUDED.source_file_id
    """
)


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None

    raw = str(value).strip().replace(",", "")

    if not raw or raw in {"-", "**", "***", "N", "(X)"}:
        return None

    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _row_dict(headers: list[str], row: list[Any]) -> dict[str, Any]:
    return dict(zip(headers, row, strict=False))


def _source_geo_id(row: dict[str, Any], geography_level: str) -> str | None:
    if geography_level == "state":
        state = row.get("state")
        return f"state:{state}" if state else None

    if geography_level == "county":
        state = row.get("state")
        county = row.get("county")
        return f"county:{state}{county}" if state and county else None

    if geography_level == "metro":
        cbsa = row.get("metropolitan statistical area/micropolitan statistical area")
        return f"cbsa:{cbsa}" if cbsa else None

    return None


def _record_to_params(
    *,
    headers: list[str],
    row: list[Any],
    geography_level: str,
    year: int,
    source_period_start: date,
    source_period_end: date,
    source_file_id: str | None,
    load_date: date,
) -> dict | None:
    row_data = _row_dict(headers, row)
    source_geo_id = _source_geo_id(row_data, geography_level)

    if not source_geo_id:
        return None

    return {
        "geography_level": geography_level,
        "source_geo_id": source_geo_id,
        "source_name": row_data.get("NAME") or "",
        "state_fips": row_data.get("state"),
        "county_fips": row_data.get("county"),
        "cbsa_code": row_data.get("metropolitan statistical area/micropolitan statistical area"),
        "year": year,
        "source_period_start": source_period_start,
        "source_period_end": source_period_end,
        "total_population": _parse_decimal(row_data.get("DP05_0001E")),
        "median_household_income": _parse_decimal(row_data.get("DP03_0062E")),
        "total_housing_units": _parse_decimal(row_data.get("DP04_0001E")),
        "occupied_housing_units": _parse_decimal(row_data.get("DP04_0002E")),
        "vacant_housing_units": _parse_decimal(row_data.get("DP04_0003E")),
        "owner_occupied_housing_units": _parse_decimal(row_data.get("DP04_0046E")),
        "renter_occupied_housing_units": _parse_decimal(row_data.get("DP04_0047E")),
        "median_gross_rent": _parse_decimal(row_data.get("DP04_0089E")),
        "rent_burden_pct": _parse_decimal(row_data.get("DP04_0142PE")),
        "source_payload": json.dumps(row_data),
        "source_file_id": source_file_id,
        "load_date": load_date,
    }


def load_census_acs_profile(
    *,
    payload: list,
    geography_level: str,
    year: int,
    source_period_start: date,
    source_period_end: date,
    source_file_id: str | None,
    load_date: date,
) -> int:
    if len(payload) <= 1:
        return 0

    headers = payload[0]
    rows = payload[1:]

    params = [
        parsed
        for row in rows
        if (
            parsed := _record_to_params(
                headers=headers,
                row=row,
                geography_level=geography_level,
                year=year,
                source_period_start=source_period_start,
                source_period_end=source_period_end,
                source_file_id=source_file_id,
                load_date=load_date,
            )
        )
        is not None
    ]

    if not params:
        return 0

    with engine.begin() as connection:
        connection.execute(UPSERT_CENSUS_ACS_PROFILE_SQL, params)

    return len(params)
