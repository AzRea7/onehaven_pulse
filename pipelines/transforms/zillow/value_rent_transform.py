from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.transforms.common.market_metric_loader import upsert_market_metrics
from pipelines.transforms.common.market_metric_record import MarketMetricRecord
from pipelines.transforms.common.transform_audit import finish_transform_run, start_transform_run


SOURCE = "zillow"
TRANSFORM_NAME = "zillow_value_rent_transform"
TARGET_TABLE = "analytics.market_monthly_metrics"


@dataclass(frozen=True)
class RawZillowRecord:
    dataset: str
    source_region_id: str
    region_name: str
    region_type: str | None
    state_name: str | None
    metro: str | None
    county_name: str | None
    period_month: date
    value: Decimal
    source_file_id: str | None


@dataclass(frozen=True)
class MappedZillowRecord:
    geo_id: str
    dataset: str
    source_region_id: str
    region_name: str
    region_type: str | None
    state_name: str | None
    metro: str | None
    county_name: str | None
    period_month: date
    value: Decimal
    source_file_id: str | None
    match_method: str
    confidence_score: Decimal


DATASET_TO_METRIC = {
    "zhvi": "zhvi",
    "zori": "zori",
}


def _normalize_text(value: str | None) -> str:
    return " ".join((value or "").strip().lower().replace(",", "").split())


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


def _quantize_value(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _quantize_pct(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def _subtract_months(value: date, months: int) -> date:
    total_months = value.year * 12 + value.month - 1 - months
    year = total_months // 12
    month = total_months % 12 + 1
    return date(year, month, 1)


def fetch_raw_zillow_records(dataset: str) -> list[RawZillowRecord]:
    if dataset not in {"zhvi", "zori"}:
        raise ValueError(f"Unsupported Zillow dataset: {dataset}")

    table_name = f"raw.zillow_{dataset}"

    sql = text(
        f"""
        WITH ranked AS (
            SELECT
                source_region_id,
                region_name,
                region_type,
                state_name,
                metro,
                county_name,
                period_month,
                value,
                source_file_id,
                ROW_NUMBER() OVER (
                    PARTITION BY source_region_id, period_month
                    ORDER BY load_date DESC, created_at DESC
                ) AS row_number
            FROM {table_name}
            WHERE value IS NOT NULL
        )
        SELECT
            source_region_id,
            region_name,
            region_type,
            state_name,
            metro,
            county_name,
            period_month,
            value,
            source_file_id
        FROM ranked
        WHERE row_number = 1
        ORDER BY source_region_id, period_month
        """
    )

    records: list[RawZillowRecord] = []

    with engine.begin() as connection:
        result = connection.execute(sql)

        for row in result.mappings():
            value = _parse_decimal(row["value"])

            if value is None:
                continue

            records.append(
                RawZillowRecord(
                    dataset=dataset,
                    source_region_id=str(row["source_region_id"]),
                    region_name=str(row["region_name"]),
                    region_type=row["region_type"],
                    state_name=row["state_name"],
                    metro=row["metro"],
                    county_name=row["county_name"],
                    period_month=row["period_month"],
                    value=value,
                    source_file_id=row["source_file_id"],
                )
            )

    return records


def _lookup_geo_id(record: RawZillowRecord) -> tuple[str, str, Decimal] | None:
    region_type = _normalize_text(record.region_type)
    region_name = _normalize_text(record.region_name)

    if region_type in {"country", "nation", "national"} or region_name in {
        "united states",
        "usa",
        "us",
    }:
        return "us", "zillow_country", Decimal("1.0000")

    if region_type in {"state"}:
        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type = 'state'
              AND (
                    lower(name) = :name
                 OR lower(display_name) = :name
                 OR lower(state_name) = :name
                 OR lower(state_code) = :name
              )
            ORDER BY geo_id
            LIMIT 1
            """
        )

        with engine.begin() as connection:
            geo_id = connection.execute(sql, {"name": region_name}).scalar_one_or_none()

        if geo_id:
            return str(geo_id), "state_name_exact", Decimal("0.9800")

    if region_type in {"msa", "metro"}:
        metro_name = _normalize_text(record.region_name)
        like_name = f"%{metro_name.split(' metro')[0]}%"

        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type IN ('metro', 'cbsa')
              AND (
                    lower(name) = :name
                 OR lower(display_name) = :name
                 OR lower(name) LIKE :like_name
                 OR lower(display_name) LIKE :like_name
              )
            ORDER BY
                CASE
                    WHEN lower(name) = :name THEN 1
                    WHEN lower(display_name) = :name THEN 2
                    ELSE 3
                END,
                geo_id
            LIMIT 1
            """
        )

        with engine.begin() as connection:
            geo_id = connection.execute(
                sql,
                {
                    "name": metro_name,
                    "like_name": like_name,
                },
            ).scalar_one_or_none()

        if geo_id:
            return str(geo_id), "metro_name_match", Decimal("0.8500")

    return None


def map_records(
    raw_records: list[RawZillowRecord],
) -> tuple[list[MappedZillowRecord], list[RawZillowRecord]]:
    mapped: list[MappedZillowRecord] = []
    unmatched: list[RawZillowRecord] = []

    cache: dict[tuple[str, str], tuple[str, str, Decimal] | None] = {}

    for record in raw_records:
        key = (_normalize_text(record.region_type), _normalize_text(record.region_name))

        if key not in cache:
            cache[key] = _lookup_geo_id(record)

        resolved = cache[key]

        if resolved is None:
            unmatched.append(record)
            continue

        geo_id, match_method, confidence_score = resolved

        mapped.append(
            MappedZillowRecord(
                geo_id=geo_id,
                dataset=record.dataset,
                source_region_id=record.source_region_id,
                region_name=record.region_name,
                region_type=record.region_type,
                state_name=record.state_name,
                metro=record.metro,
                county_name=record.county_name,
                period_month=record.period_month,
                value=record.value,
                source_file_id=record.source_file_id,
                match_method=match_method,
                confidence_score=confidence_score,
            )
        )

    return mapped, unmatched


def _base_metric_name(dataset: str) -> str:
    return DATASET_TO_METRIC[dataset]


def _build_level_records(
    mapped_records: list[MappedZillowRecord],
    transform_run_id: str,
) -> list[MarketMetricRecord]:
    records: list[MarketMetricRecord] = []

    for record in mapped_records:
        metric_name = _base_metric_name(record.dataset)

        records.append(
            MarketMetricRecord(
                geo_id=record.geo_id,
                period_month=record.period_month,
                metric_name=metric_name,
                metric_value=_quantize_value(record.value),
                metric_unit="usd",
                source=SOURCE,
                dataset=record.dataset,
                source_file_id=record.source_file_id,
                pipeline_run_id=transform_run_id,
                source_value=_quantize_value(record.value),
                source_period=record.period_month,
                period_grain="monthly",
                transformation_notes=f"Transformed Zillow {record.dataset.upper()} value.",
                source_flags={
                    "source_region_id": record.source_region_id,
                    "region_name": record.region_name,
                    "region_type": record.region_type,
                    "state_name": record.state_name,
                    "metro": record.metro,
                    "county_name": record.county_name,
                    "match_method": record.match_method,
                    "confidence_score": str(record.confidence_score),
                },
                quality_flags={
                    "period_normalized_to_month_start": True,
                },
            )
        )

    return records


def _build_growth_records(
    mapped_records: list[MappedZillowRecord],
    transform_run_id: str,
) -> list[MarketMetricRecord]:
    grouped: dict[tuple[str, str], list[MappedZillowRecord]] = defaultdict(list)

    for record in mapped_records:
        grouped[(record.geo_id, record.dataset)].append(record)

    records: list[MarketMetricRecord] = []

    for (_geo_id, dataset), group in grouped.items():
        sorted_group = sorted(group, key=lambda item: item.period_month)
        by_period = {record.period_month: record for record in sorted_group}
        base_metric = _base_metric_name(dataset)

        for record in sorted_group:
            previous_record = by_period.get(_subtract_months(record.period_month, 1))
            prior_year_record = by_period.get(_subtract_months(record.period_month, 12))

            if previous_record and previous_record.value != 0:
                mom = ((record.value - previous_record.value) / previous_record.value) * Decimal("100")
                records.append(
                    MarketMetricRecord(
                        geo_id=record.geo_id,
                        period_month=record.period_month,
                        metric_name=f"{base_metric}_mom",
                        metric_value=_quantize_pct(mom),
                        metric_unit="percent",
                        source=SOURCE,
                        dataset=dataset,
                        source_file_id=record.source_file_id,
                        pipeline_run_id=transform_run_id,
                        source_value=_quantize_pct(mom),
                        source_period=record.period_month,
                        period_grain="monthly",
                        transformation_notes=f"Calculated Zillow {dataset.upper()} month-over-month growth.",
                        source_flags={
                            "source_region_id": record.source_region_id,
                            "region_name": record.region_name,
                            "calculation_method": "month_over_month",
                            "current_value": str(record.value),
                            "prior_value": str(previous_record.value),
                        },
                        quality_flags={
                            "requires_prior_month": True,
                            "prior_month_available": True,
                        },
                    )
                )

            if prior_year_record and prior_year_record.value != 0:
                yoy = ((record.value - prior_year_record.value) / prior_year_record.value) * Decimal("100")
                records.append(
                    MarketMetricRecord(
                        geo_id=record.geo_id,
                        period_month=record.period_month,
                        metric_name=f"{base_metric}_yoy",
                        metric_value=_quantize_pct(yoy),
                        metric_unit="percent",
                        source=SOURCE,
                        dataset=dataset,
                        source_file_id=record.source_file_id,
                        pipeline_run_id=transform_run_id,
                        source_value=_quantize_pct(yoy),
                        source_period=record.period_month,
                        period_grain="monthly",
                        transformation_notes=f"Calculated Zillow {dataset.upper()} year-over-year growth.",
                        source_flags={
                            "source_region_id": record.source_region_id,
                            "region_name": record.region_name,
                            "calculation_method": "year_over_year",
                            "current_value": str(record.value),
                            "prior_year_value": str(prior_year_record.value),
                        },
                        quality_flags={
                            "requires_12_month_lookback": True,
                            "prior_year_available": True,
                        },
                    )
                )

    return records


def build_records(
    raw_records: list[RawZillowRecord],
    transform_run_id: str,
) -> tuple[list[MarketMetricRecord], list[RawZillowRecord]]:
    mapped_records, unmatched_records = map_records(raw_records)

    metric_records = [
        *_build_level_records(mapped_records, transform_run_id),
        *_build_growth_records(mapped_records, transform_run_id),
    ]

    return metric_records, unmatched_records


def main() -> None:
    transform_run_id = start_transform_run(
        transform_name=TRANSFORM_NAME,
        source=SOURCE,
        dataset="zhvi_zori",
        target_table=TARGET_TABLE,
        metadata={
            "target_metrics": [
                "zhvi",
                "zhvi_yoy",
                "zhvi_mom",
                "zori",
                "zori_yoy",
                "zori_mom",
            ],
        },
    )

    try:
        raw_records = [
            *fetch_raw_zillow_records("zhvi"),
            *fetch_raw_zillow_records("zori"),
        ]
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
            else f"{len(unmatched_records)} Zillow records had unmatched geography.",
        )

        print(
            f"Zillow value/rent transform complete. "
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
