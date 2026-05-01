from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text

from pipelines.common.db import engine


UPSERT_FHFA_HPI_SQL = text(
    """
    INSERT INTO raw.fhfa_hpi (
        geo_name,
        geo_type,
        period,
        frequency,
        hpi,
        source_file_id,
        load_date
    )
    VALUES (
        :geo_name,
        :geo_type,
        :period,
        :frequency,
        :hpi,
        :source_file_id,
        :load_date
    )
    ON CONFLICT (
        geo_name,
        geo_type,
        period,
        frequency,
        load_date
    )
    DO UPDATE SET
        hpi = EXCLUDED.hpi,
        source_file_id = EXCLUDED.source_file_id
    """
)


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None

    text_value = str(value).strip()

    if not text_value or text_value == ".":
        return None

    try:
        return Decimal(text_value)
    except InvalidOperation:
        return None


def _parse_year_month(year_value: Any, period_value: Any) -> date | None:
    if year_value is None or period_value is None:
        return None

    try:
        year = int(str(year_value).strip())
        month = int(str(period_value).strip())
    except ValueError:
        return None

    if month < 1 or month > 12:
        return None

    return date(year, month, 1)


def _normalize_geo_type(value: Any) -> str:
    raw = str(value or "").strip().lower()

    if raw in {"usa", "us", "national", "nation"}:
        return "national"

    if raw in {"state", "states"}:
        return "state"

    if raw in {"msa", "cbsa", "metro", "metropolitan statistical area"}:
        return "metro"

    return raw or "unknown"


def _record_to_params(
    record: dict,
    *,
    source_file_id: str | None,
    load_date: date,
) -> dict | None:
    geo_name = record.get("place_name")
    geo_type = _normalize_geo_type(record.get("level"))
    period = _parse_year_month(record.get("yr"), record.get("period"))
    frequency = str(record.get("frequency") or "monthly").strip().lower()

    hpi = (
        _parse_decimal(record.get("index_sa"))
        or _parse_decimal(record.get("index_nsa"))
        or _parse_decimal(record.get("hpi"))
    )

    if not geo_name or not geo_type or period is None or hpi is None:
        return None

    return {
        "geo_name": str(geo_name).strip(),
        "geo_type": geo_type,
        "period": period,
        "frequency": frequency,
        "hpi": hpi,
        "source_file_id": source_file_id,
        "load_date": load_date,
    }


def load_fhfa_hpi_records(
    *,
    records: list[dict],
    source_file_id: str | None,
    load_date: date,
) -> int:
    params = [
        parsed
        for record in records
        if (
            parsed := _record_to_params(
                record,
                source_file_id=source_file_id,
                load_date=load_date,
            )
        )
        is not None
    ]

    if not params:
        return 0

    with engine.begin() as connection:
        connection.execute(UPSERT_FHFA_HPI_SQL, params)

    return len(params)
