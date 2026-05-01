import json
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.extractors.bls_laus.config import BlsLausDataset


UPSERT_BLS_LAUS_OBSERVATION_SQL = text(
    """
    INSERT INTO raw.bls_laus_observations (
        series_id,
        geography_level,
        measure,
        geo_reference,
        year,
        period,
        period_month,
        value,
        footnotes,
        source_file_id,
        load_date
    )
    VALUES (
        :series_id,
        :geography_level,
        :measure,
        :geo_reference,
        :year,
        :period,
        :period_month,
        :value,
        CAST(:footnotes AS JSONB),
        :source_file_id,
        :load_date
    )
    ON CONFLICT (
        series_id,
        year,
        period,
        load_date
    )
    DO UPDATE SET
        geography_level = EXCLUDED.geography_level,
        measure = EXCLUDED.measure,
        geo_reference = EXCLUDED.geo_reference,
        period_month = EXCLUDED.period_month,
        value = EXCLUDED.value,
        footnotes = EXCLUDED.footnotes,
        source_file_id = EXCLUDED.source_file_id
    """
)


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None

    raw = str(value).strip().replace(",", "")

    if not raw or raw in {"-", ".", "N/A"}:
        return None

    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _period_to_month(year: int, period: str) -> date | None:
    if not period.startswith("M"):
        return None

    try:
        month = int(period[1:])
    except ValueError:
        return None

    if month < 1 or month > 12:
        return None

    return date(year, month, 1)


def _series_metadata(dataset: BlsLausDataset) -> dict[str, dict[str, str]]:
    return {
        series.series_id: {
            "geography_level": series.geography_level,
            "measure": series.measure,
            "geo_reference": series.geo_reference,
        }
        for series in dataset.series
    }


def load_bls_laus_observations(
    *,
    payload: dict,
    dataset: BlsLausDataset,
    source_file_id: str | None,
    load_date: date,
) -> int:
    metadata_by_series_id = _series_metadata(dataset)

    series_list = (
        payload.get("response", {})
        .get("Results", {})
        .get("series", [])
    )

    params: list[dict] = []

    for series_payload in series_list:
        series_id = series_payload.get("seriesID")
        metadata = metadata_by_series_id.get(series_id)

        if not series_id or metadata is None:
            continue

        for observation in series_payload.get("data", []):
            period = observation.get("period")
            year_raw = observation.get("year")

            if not period or not year_raw:
                continue

            try:
                year = int(year_raw)
            except ValueError:
                continue

            period_month = _period_to_month(year, period)

            if period_month is None:
                continue

            params.append(
                {
                    "series_id": series_id,
                    "geography_level": metadata["geography_level"],
                    "measure": metadata["measure"],
                    "geo_reference": metadata["geo_reference"],
                    "year": year,
                    "period": period,
                    "period_month": period_month,
                    "value": _parse_decimal(observation.get("value")),
                    "footnotes": json.dumps(observation.get("footnotes") or []),
                    "source_file_id": source_file_id,
                    "load_date": load_date,
                }
            )

    if not params:
        return 0

    with engine.begin() as connection:
        connection.execute(UPSERT_BLS_LAUS_OBSERVATION_SQL, params)

    return len(params)
