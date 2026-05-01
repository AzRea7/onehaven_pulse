import json
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.extractors.hud_usps.config import HudUspsDataset


UPSERT_RAW_SQL = text(
    """
    INSERT INTO raw.hud_usps_zip_crosswalk (
        crosswalk_type,
        year,
        quarter,
        query,
        zip_code,
        tract_geoid,
        county_fips,
        cbsa_code,
        target_key,
        residential_ratio,
        business_ratio,
        other_ratio,
        total_ratio,
        source_payload,
        source_file_id,
        load_date
    )
    VALUES (
        :crosswalk_type,
        :year,
        :quarter,
        :query,
        :zip_code,
        :tract_geoid,
        :county_fips,
        :cbsa_code,
        :target_key,
        :residential_ratio,
        :business_ratio,
        :other_ratio,
        :total_ratio,
        CAST(:source_payload AS JSONB),
        :source_file_id,
        :load_date
    )
    ON CONFLICT (
        crosswalk_type,
        year,
        quarter,
        zip_code,
        target_key,
        load_date
    )
    DO UPDATE SET
        residential_ratio = EXCLUDED.residential_ratio,
        business_ratio = EXCLUDED.business_ratio,
        other_ratio = EXCLUDED.other_ratio,
        total_ratio = EXCLUDED.total_ratio,
        source_payload = EXCLUDED.source_payload,
        source_file_id = EXCLUDED.source_file_id
    """
)

UPSERT_GEO_SQL = text(
    """
    INSERT INTO geo.zip_geo_crosswalk (
        zip_code,
        target_geo_id,
        target_geo_type,
        allocation_ratio,
        source,
        source_year,
        source_quarter,
        source_file_id,
        load_date
    )
    VALUES (
        :zip_code,
        :target_geo_id,
        :target_geo_type,
        :allocation_ratio,
        'hud_usps',
        :source_year,
        :source_quarter,
        :source_file_id,
        :load_date
    )
    ON CONFLICT (
        zip_code,
        target_geo_id,
        source_year,
        source_quarter,
        source
    )
    DO UPDATE SET
        allocation_ratio = EXCLUDED.allocation_ratio,
        source_file_id = EXCLUDED.source_file_id,
        load_date = EXCLUDED.load_date,
        updated_at = now()
    """
)


def _blocks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data")

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        return [data]

    return []


def _results(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for block in _blocks(payload):
        result = block.get("results", [])

        if isinstance(result, list):
            rows.extend(row for row in result if isinstance(row, dict))

    return rows


def _first(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    lower = {str(k).lower(): v for k, v in row.items()}

    for key in keys:
        value = row.get(key)

        if value not in (None, ""):
            return value

        value = lower.get(key.lower())

        if value not in (None, ""):
            return value

    return None


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None

    raw = str(value).strip().replace(",", "")

    if not raw or raw.lower() in {"nan", "none", "null"}:
        return None

    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _clean_zip(value: Any) -> str | None:
    if value is None:
        return None

    raw = str(value).strip()

    if not raw:
        return None

    return raw.zfill(5)[:5]


def _target_from_row(dataset: HudUspsDataset, row: dict[str, Any]) -> tuple[str, str] | None:
    if dataset.crosswalk_type == "zip_county":
        county = _first(row, ("county", "county_fips", "geoid", "county_geoid"))

        if county:
            return f"county:{str(county).strip().zfill(5)}", "county"

    if dataset.crosswalk_type == "zip_cbsa":
        cbsa = _first(row, ("cbsa", "cbsa_code", "geoid", "cbsa_geoid"))

        if cbsa:
            return f"cbsa:{str(cbsa).strip()}", "cbsa"

    if dataset.crosswalk_type == "zip_tract":
        tract = _first(row, ("tract", "tract_geoid", "geoid"))

        if tract:
            return f"tract:{str(tract).strip()}", "tract"

    return None


def load_hud_usps_crosswalk(
    *,
    payload: dict[str, Any],
    dataset: HudUspsDataset,
    source_file_id: str | None,
    load_date: date,
) -> int:
    raw_params: list[dict[str, Any]] = []
    geo_params: list[dict[str, Any]] = []

    for row in _results(payload):
        zip_code = _clean_zip(_first(row, ("zip", "zip_code", "zip5")))

        if not zip_code:
            continue

        if dataset.crosswalk_type == "zip_tract":
            tract_geoid = _first(row, ("tract", "tract_geoid", "geoid"))
            county_fips = None
            cbsa_code = None
        elif dataset.crosswalk_type == "zip_county":
            county_fips = _first(row, ("county", "county_fips", "county_geoid", "geoid"))
            tract_geoid = None
            cbsa_code = None
        elif dataset.crosswalk_type == "zip_cbsa":
            cbsa_code = _first(row, ("cbsa", "cbsa_code", "cbsa_geoid", "geoid"))
            tract_geoid = None
            county_fips = None
        else:
            county_fips = _first(row, ("county", "county_fips", "county_geoid"))
            cbsa_code = _first(row, ("cbsa", "cbsa_code", "cbsa_geoid"))
            tract_geoid = _first(row, ("tract", "tract_geoid", "geoid"))

        res_ratio = _decimal(_first(row, ("res_ratio", "residential_ratio", "resratio")))
        bus_ratio = _decimal(_first(row, ("bus_ratio", "business_ratio", "busratio")))
        oth_ratio = _decimal(_first(row, ("oth_ratio", "other_ratio", "othratio")))
        total_ratio = _decimal(_first(row, ("tot_ratio", "total_ratio", "totratio")))

        raw_params.append(
            {
                "crosswalk_type": dataset.crosswalk_type,
                "year": dataset.year,
                "quarter": dataset.quarter,
                "query": dataset.query,
                "zip_code": zip_code,
                "tract_geoid": str(tract_geoid).strip() if tract_geoid else None,
                "county_fips": str(county_fips).strip().zfill(5) if county_fips else None,
                "cbsa_code": str(cbsa_code).strip() if cbsa_code else None,
                "target_key": (
                    str(tract_geoid).strip()
                    if tract_geoid
                    else str(county_fips).strip().zfill(5)
                    if county_fips
                    else str(cbsa_code).strip()
                    if cbsa_code
                    else "unknown"
                ),
                "residential_ratio": res_ratio,
                "business_ratio": bus_ratio,
                "other_ratio": oth_ratio,
                "total_ratio": total_ratio,
                "source_payload": json.dumps(row, default=str),
                "source_file_id": source_file_id,
                "load_date": load_date,
            }
        )

        target = _target_from_row(dataset, row)

        if target is None:
            continue

        target_geo_id, target_geo_type = target
        allocation_ratio = res_ratio or total_ratio or Decimal("1")

        geo_params.append(
            {
                "zip_code": zip_code,
                "target_geo_id": target_geo_id,
                "target_geo_type": target_geo_type,
                "allocation_ratio": allocation_ratio,
                "source_year": dataset.year,
                "source_quarter": dataset.quarter,
                "source_file_id": source_file_id,
                "load_date": load_date,
            }
        )

    if not raw_params:
        return 0

    with engine.begin() as connection:
        connection.execute(UPSERT_RAW_SQL, raw_params)

        if geo_params:
            connection.execute(UPSERT_GEO_SQL, geo_params)

    return len(raw_params)
