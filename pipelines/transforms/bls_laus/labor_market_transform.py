from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.transforms.common.market_metric_loader import upsert_market_metrics
from pipelines.transforms.common.market_metric_record import MarketMetricRecord
from pipelines.transforms.common.transform_audit import finish_transform_run, start_transform_run


SOURCE = "bls_laus"
DATASET = "labor_market"
TRANSFORM_NAME = "bls_laus_labor_market_transform"
TARGET_TABLE = "analytics.market_monthly_metrics"


MEASURE_TO_METRIC: dict[str, tuple[str, str]] = {
    "unemployment_rate": ("unemployment_rate", "percent"),
    "labor_force": ("labor_force", "count"),
    "employment": ("employment", "count"),
    "unemployment_count": ("unemployment_count", "count"),
}


@dataclass(frozen=True)
class RawBlsLausObservation:
    series_id: str
    geography_level: str
    measure: str
    geo_reference: str
    period_month: date
    value: Decimal
    source_file_id: str | None


@dataclass(frozen=True)
class MappedBlsLausObservation:
    geo_id: str
    series_id: str
    geography_level: str
    measure: str
    geo_reference: str
    period_month: date
    value: Decimal
    source_file_id: str | None
    match_method: str
    confidence_score: Decimal


def _normalize_text(value: str | None) -> str:
    return " ".join((value or "").strip().lower().replace(",", "").split())


def _parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None

    raw = str(value).strip()

    if not raw:
        return None

    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _quantize_metric(value: Decimal, metric_name: str) -> Decimal:
    if metric_name == "unemployment_rate":
        return value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def fetch_raw_bls_laus_observations() -> list[RawBlsLausObservation]:
    sql = text(
        """
        WITH ranked AS (
            SELECT
                series_id,
                geography_level,
                measure,
                geo_reference,
                period_month,
                value,
                source_file_id,
                ROW_NUMBER() OVER (
                    PARTITION BY series_id, period_month
                    ORDER BY load_date DESC, created_at DESC
                ) AS row_number
            FROM raw.bls_laus_observations
            WHERE value IS NOT NULL
        )
        SELECT
            series_id,
            geography_level,
            measure,
            geo_reference,
            period_month,
            value,
            source_file_id
        FROM ranked
        WHERE row_number = 1
        ORDER BY geography_level, geo_reference, measure, period_month
        """
    )

    records: list[RawBlsLausObservation] = []

    with engine.begin() as connection:
        result = connection.execute(sql)

        for row in result.mappings():
            value = _parse_decimal(row["value"])

            if value is None:
                continue

            records.append(
                RawBlsLausObservation(
                    series_id=row["series_id"],
                    geography_level=row["geography_level"],
                    measure=row["measure"],
                    geo_reference=row["geo_reference"],
                    period_month=row["period_month"],
                    value=value,
                    source_file_id=row["source_file_id"],
                )
            )

    return records


def _lookup_geo_id(record: RawBlsLausObservation) -> tuple[str, str, Decimal] | None:
    geography_level = _normalize_text(record.geography_level)
    geo_reference = record.geo_reference.strip()

    if geography_level == "national" or geo_reference.upper() in {"US", "USA"}:
        return "us", "national_exact", Decimal("1.0000")

    if geography_level == "state":
        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type = 'state'
              AND lower(state_code) = :state_code
            LIMIT 1
            """
        )

        with engine.begin() as connection:
            geo_id = connection.execute(
                sql,
                {"state_code": geo_reference.lower()},
            ).scalar_one_or_none()

        if geo_id:
            return str(geo_id), "state_code_exact", Decimal("1.0000")

    return None


def map_records(
    raw_records: list[RawBlsLausObservation],
) -> tuple[list[MappedBlsLausObservation], list[RawBlsLausObservation]]:
    mapped: list[MappedBlsLausObservation] = []
    unmatched: list[RawBlsLausObservation] = []

    for record in raw_records:
        resolved = _lookup_geo_id(record)

        if resolved is None:
            unmatched.append(record)
            continue

        geo_id, match_method, confidence_score = resolved

        mapped.append(
            MappedBlsLausObservation(
                geo_id=geo_id,
                series_id=record.series_id,
                geography_level=record.geography_level,
                measure=record.measure,
                geo_reference=record.geo_reference,
                period_month=record.period_month,
                value=record.value,
                source_file_id=record.source_file_id,
                match_method=match_method,
                confidence_score=confidence_score,
            )
        )

    return mapped, unmatched


def build_records(
    raw_records: list[RawBlsLausObservation],
    transform_run_id: str,
) -> tuple[list[MarketMetricRecord], list[RawBlsLausObservation]]:
    mapped_records, unmatched_records = map_records(raw_records)
    metric_records: list[MarketMetricRecord] = []

    for record in mapped_records:
        if record.measure not in MEASURE_TO_METRIC:
            continue

        metric_name, metric_unit = MEASURE_TO_METRIC[record.measure]
        normalized_value = _quantize_metric(record.value, metric_name)

        metric_records.append(
            MarketMetricRecord(
                geo_id=record.geo_id,
                period_month=record.period_month,
                metric_name=metric_name,
                metric_value=normalized_value,
                metric_unit=metric_unit,
                source=SOURCE,
                dataset=DATASET,
                source_file_id=record.source_file_id,
                pipeline_run_id=transform_run_id,
                source_value=normalized_value,
                source_period=record.period_month,
                period_grain="monthly",
                transformation_notes=f"Transformed BLS LAUS {record.measure}.",
                source_flags={
                    "series_id": record.series_id,
                    "geography_level": record.geography_level,
                    "geo_reference": record.geo_reference,
                    "measure": record.measure,
                    "match_method": record.match_method,
                    "confidence_score": str(record.confidence_score),
                },
                quality_flags={
                    "period_normalized_to_month_start": True,
                },
            )
        )

    return metric_records, unmatched_records


def main() -> None:
    transform_run_id = start_transform_run(
        transform_name=TRANSFORM_NAME,
        source=SOURCE,
        dataset=DATASET,
        target_table=TARGET_TABLE,
        metadata={
            "target_metrics": sorted(
                {metric_name for metric_name, _unit in MEASURE_TO_METRIC.values()}
            ),
        },
    )

    try:
        raw_records = fetch_raw_bls_laus_observations()
        metric_records, unmatched_records = build_records(raw_records, transform_run_id)
        loaded_count = upsert_market_metrics(metric_records)

        finish_transform_run(
            run_id=transform_run_id,
            status="success",
            records_extracted=len(raw_records),
            records_loaded=loaded_count,
            records_failed=len(unmatched_records),
            error_message=None
            if not unmatched_records
            else f"{len(unmatched_records)} BLS LAUS records had unmatched geography.",
        )

        print(
            f"BLS LAUS labor market transform complete. "
            f"Raw records: {len(raw_records)}. "
            f"Loaded metrics: {loaded_count}. "
            f"Unmatched records: {len(unmatched_records)}. "
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
