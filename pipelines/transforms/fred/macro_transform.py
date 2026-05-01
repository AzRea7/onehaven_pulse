from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.transforms.common.market_metric_loader import upsert_market_metrics
from pipelines.transforms.common.market_metric_record import MarketMetricRecord
from pipelines.transforms.common.transform_audit import finish_transform_run, start_transform_run


SOURCE = "fred"
DATASET = "macro_series"
TRANSFORM_NAME = "fred_macro_monthly_transform"
TARGET_TABLE = "analytics.market_monthly_metrics"
NATIONAL_GEO_ID = "us"


SERIES_TO_METRIC: dict[str, str] = {
    "MORTGAGE30US": "mortgage_rate_30y",
    "CPIAUCSL": "cpi",
    "UNRATE": "unemployment_rate",
    "FEDFUNDS": "fed_funds_rate",
    "USREC": "recession_indicator",
    "DGS2": "treasury_2yr_rate",
    "DGS5": "treasury_5yr_rate",
    "DGS10": "treasury_10yr_rate",
    "DGS30": "treasury_30yr_rate",
    "T10Y2Y": "treasury_10yr_2yr_spread",
    "T10Y3M": "treasury_10yr_3mo_spread",
}


SERIES_TO_UNIT: dict[str, str] = {
    "MORTGAGE30US": "percent",
    "CPIAUCSL": "index",
    "UNRATE": "percent",
    "FEDFUNDS": "percent",
    "USREC": "binary",
    "DGS2": "percent",
    "DGS5": "percent",
    "DGS10": "percent",
    "DGS30": "percent",
    "T10Y2Y": "percentage_points",
    "T10Y3M": "percentage_points",
}


DAILY_OR_WEEKLY_SERIES = {
    "MORTGAGE30US",
    "DGS2",
    "DGS5",
    "DGS10",
    "DGS30",
    "T10Y2Y",
    "T10Y3M",
}


MONTHLY_POINT_SERIES = {
    "CPIAUCSL",
    "UNRATE",
    "FEDFUNDS",
}


RECESSION_SERIES = {
    "USREC",
}


@dataclass(frozen=True)
class RawFredObservation:
    series_id: str
    observation_date: date
    value: Decimal
    source_file_id: str | None


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def _parse_decimal(raw_value: object) -> Decimal | None:
    if raw_value is None:
        return None

    value = str(raw_value).strip()

    if not value or value == ".":
        return None

    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def _quantize_metric(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def fetch_raw_fred_observations() -> list[RawFredObservation]:
    sql = text(
        """
        WITH ranked AS (
            SELECT
                series_id,
                observation_date,
                value,
                source_file_id,
                ROW_NUMBER() OVER (
                    PARTITION BY series_id, observation_date
                    ORDER BY load_date DESC, created_at DESC
                ) AS row_number
            FROM raw.fred_observations
            WHERE series_id = ANY(:series_ids)
        )
        SELECT
            series_id,
            observation_date,
            value,
            source_file_id
        FROM ranked
        WHERE row_number = 1
        ORDER BY series_id, observation_date
        """
    )

    rows: list[RawFredObservation] = []

    with engine.begin() as connection:
        result = connection.execute(sql, {"series_ids": list(SERIES_TO_METRIC)})

        for row in result.mappings():
            parsed_value = _parse_decimal(row["value"])

            if parsed_value is None:
                continue

            rows.append(
                RawFredObservation(
                    series_id=row["series_id"],
                    observation_date=row["observation_date"],
                    value=parsed_value,
                    source_file_id=row["source_file_id"],
                )
            )

    return rows


def _average(values: list[Decimal]) -> Decimal:
    return sum(values) / Decimal(len(values))


def build_records(
    raw_observations: list[RawFredObservation],
    transform_run_id: str,
) -> list[MarketMetricRecord]:
    grouped: dict[tuple[str, date], list[RawFredObservation]] = defaultdict(list)

    for observation in raw_observations:
        grouped[(observation.series_id, _month_start(observation.observation_date))].append(
            observation
        )

    records: list[MarketMetricRecord] = []

    for (series_id, period_month), observations in sorted(grouped.items()):
        metric_name = SERIES_TO_METRIC[series_id]
        metric_unit = SERIES_TO_UNIT[series_id]
        source_file_id = observations[-1].source_file_id
        source_period = max(observation.observation_date for observation in observations)

        values = [observation.value for observation in observations]

        if series_id in DAILY_OR_WEEKLY_SERIES:
            normalized_value = _average(values)
            aggregation_method = "monthly_average"
        elif series_id in MONTHLY_POINT_SERIES:
            latest_observation = max(observations, key=lambda item: item.observation_date)
            normalized_value = latest_observation.value
            aggregation_method = "monthly_point"
        elif series_id in RECESSION_SERIES:
            normalized_value = max(values)
            aggregation_method = "monthly_max"
        else:
            raise ValueError(f"Unhandled FRED series_id: {series_id}")

        records.append(
            MarketMetricRecord(
                geo_id=NATIONAL_GEO_ID,
                period_month=period_month,
                metric_name=metric_name,
                metric_value=_quantize_metric(normalized_value),
                metric_unit=metric_unit,
                source=SOURCE,
                dataset=DATASET,
                source_file_id=source_file_id,
                pipeline_run_id=transform_run_id,
                source_value=_quantize_metric(normalized_value),
                source_period=source_period,
                period_grain="monthly",
                transformation_notes=(
                    f"Transformed FRED {series_id} to {metric_name} using "
                    f"{aggregation_method}."
                ),
                source_flags={
                    "series_id": series_id,
                    "aggregation_method": aggregation_method,
                    "observation_count": len(observations),
                },
                quality_flags={
                    "invalid_values_dropped": False,
                    "period_normalized_to_month_start": True,
                },
            )
        )

    return records


def main() -> None:
    transform_run_id = start_transform_run(
        transform_name=TRANSFORM_NAME,
        source=SOURCE,
        dataset=DATASET,
        target_table=TARGET_TABLE,
        metadata={
            "series_ids": sorted(SERIES_TO_METRIC),
            "geo_id": NATIONAL_GEO_ID,
        },
    )

    try:
        raw_observations = fetch_raw_fred_observations()
        records = build_records(raw_observations, transform_run_id)
        loaded_count = upsert_market_metrics(records)

        finish_transform_run(
            run_id=transform_run_id,
            status="success",
            records_extracted=len(raw_observations),
            records_loaded=loaded_count,
            records_failed=0,
        )

        print(
            f"FRED macro transform complete. "
            f"Raw observations: {len(raw_observations)}. "
            f"Loaded metrics: {loaded_count}. "
            f"Run ID: {transform_run_id}"
        )

    except Exception as exc:
        finish_transform_run(
            run_id=transform_run_id,
            status="failed",
            records_extracted=None,
            records_loaded=None,
            records_failed=None,
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()
