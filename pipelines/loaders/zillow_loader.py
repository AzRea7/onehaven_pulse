import csv
from datetime import date
from decimal import Decimal, InvalidOperation
from io import StringIO
from typing import Any

from sqlalchemy import text

from pipelines.common.db import engine


UPSERT_ZHVI_SQL = text(
    """
    INSERT INTO raw.zillow_zhvi (
        source_region_id,
        region_name,
        region_type,
        state_name,
        metro,
        county_name,
        period_month,
        value,
        source_file_id,
        load_date
    )
    VALUES (
        :source_region_id,
        :region_name,
        :region_type,
        :state_name,
        :metro,
        :county_name,
        :period_month,
        :value,
        :source_file_id,
        :load_date
    )
    ON CONFLICT (
        source_region_id,
        period_month,
        load_date
    )
    DO UPDATE SET
        region_name = EXCLUDED.region_name,
        region_type = EXCLUDED.region_type,
        state_name = EXCLUDED.state_name,
        metro = EXCLUDED.metro,
        county_name = EXCLUDED.county_name,
        value = EXCLUDED.value,
        source_file_id = EXCLUDED.source_file_id
    """
)


UPSERT_ZORI_SQL = text(
    """
    INSERT INTO raw.zillow_zori (
        source_region_id,
        region_name,
        region_type,
        state_name,
        metro,
        county_name,
        period_month,
        value,
        source_file_id,
        load_date
    )
    VALUES (
        :source_region_id,
        :region_name,
        :region_type,
        :state_name,
        :metro,
        :county_name,
        :period_month,
        :value,
        :source_file_id,
        :load_date
    )
    ON CONFLICT (
        source_region_id,
        period_month,
        load_date
    )
    DO UPDATE SET
        region_name = EXCLUDED.region_name,
        region_type = EXCLUDED.region_type,
        state_name = EXCLUDED.state_name,
        metro = EXCLUDED.metro,
        county_name = EXCLUDED.county_name,
        value = EXCLUDED.value,
        source_file_id = EXCLUDED.source_file_id
    """
)


def _decode_csv(content: bytes) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return content.decode("latin-1")


def _parse_period_month(value: str) -> date | None:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None

    return date(parsed.year, parsed.month, 1)


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None

    text_value = str(value).strip().replace(",", "")

    if not text_value or text_value == ".":
        return None

    try:
        return Decimal(text_value)
    except InvalidOperation:
        return None


def _date_columns(fieldnames: list[str]) -> list[str]:
    return [fieldname for fieldname in fieldnames if _parse_period_month(fieldname) is not None]


def _row_identity(row: dict) -> dict:
    return {
        "source_region_id": str(row.get("RegionID") or row.get("RegionId") or "").strip(),
        "region_name": str(row.get("RegionName") or "").strip(),
        "region_type": str(row.get("RegionType") or "").strip(),
        "state_name": str(row.get("StateName") or row.get("State") or "").strip() or None,
        "metro": str(row.get("Metro") or "").strip() or None,
        "county_name": str(row.get("CountyName") or "").strip() or None,
    }


def parse_zillow_wide_csv(
    *,
    content: bytes,
    source_file_id: str | None,
    load_date: date,
) -> list[dict]:
    text = _decode_csv(content)
    reader = csv.DictReader(StringIO(text))
    fieldnames = reader.fieldnames or []
    date_columns = _date_columns(fieldnames)

    params: list[dict] = []

    for row in reader:
        identity = _row_identity(row)

        if not identity["source_region_id"] or not identity["region_name"]:
            continue

        for column in date_columns:
            period_month = _parse_period_month(column)

            if period_month is None:
                continue

            params.append(
                {
                    **identity,
                    "period_month": period_month,
                    "value": _parse_decimal(row.get(column)),
                    "source_file_id": source_file_id,
                    "load_date": load_date,
                }
            )

    return params


def load_zillow_dataset(
    *,
    dataset: str,
    content: bytes,
    source_file_id: str | None,
    load_date: date,
) -> int:
    params = parse_zillow_wide_csv(
        content=content,
        source_file_id=source_file_id,
        load_date=load_date,
    )

    if not params:
        return 0

    if dataset == "zhvi":
        sql = UPSERT_ZHVI_SQL
    elif dataset == "zori":
        sql = UPSERT_ZORI_SQL
    else:
        raise ValueError(f"Unsupported Zillow dataset: {dataset}")

    with engine.begin() as connection:
        connection.execute(sql, params)

    return len(params)
