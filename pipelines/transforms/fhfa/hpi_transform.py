from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.transforms.common.market_metric_loader import upsert_market_metrics
from pipelines.transforms.common.market_metric_record import MarketMetricRecord
from pipelines.transforms.common.transform_audit import finish_transform_run, start_transform_run


SOURCE = "fhfa"
DATASET = "hpi"
TRANSFORM_NAME = "fhfa_hpi_transform"
TARGET_TABLE = "analytics.market_monthly_metrics"


@dataclass(frozen=True)
class RawFhfaHpiRecord:
    source_geo_name: str
    source_geo_type: str
    period: date
    frequency: str
    hpi: Decimal
    source_file_id: str | None


@dataclass(frozen=True)
class MappedFhfaHpiRecord:
    geo_id: str
    source_geo_name: str
    source_geo_type: str
    period: date
    frequency: str
    hpi: Decimal
    source_file_id: str | None
    match_method: str
    confidence_score: Decimal


def _normalize_text(value: str | None) -> str:
    return " ".join((value or "").strip().lower().replace(",", "").split())


def _period_month(value: date) -> date:
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
    return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def fetch_raw_fhfa_hpi() -> list[RawFhfaHpiRecord]:
    sql = text(
        """
        WITH ranked AS (
            SELECT
                geo_name,
                geo_type,
                period,
                frequency,
                hpi,
                source_file_id,
                ROW_NUMBER() OVER (
                    PARTITION BY geo_name, geo_type, period, frequency
                    ORDER BY load_date DESC, created_at DESC
                ) AS row_number
            FROM raw.fhfa_hpi
            WHERE hpi IS NOT NULL
        )
        SELECT
            geo_name,
            geo_type,
            period,
            frequency,
            hpi,
            source_file_id
        FROM ranked
        WHERE row_number = 1
        ORDER BY geo_type, geo_name, period
        """
    )

    rows: list[RawFhfaHpiRecord] = []

    with engine.begin() as connection:
        result = connection.execute(sql)

        for row in result.mappings():
            hpi = _parse_decimal(row["hpi"])

            if hpi is None:
                continue

            rows.append(
                RawFhfaHpiRecord(
                    source_geo_name=row["geo_name"],
                    source_geo_type=row["geo_type"],
                    period=row["period"],
                    frequency=row["frequency"],
                    hpi=hpi,
                    source_file_id=row["source_file_id"],
                )
            )

    return rows


def _resolve_national_geo_id(source_geo_name: str, source_geo_type: str) -> str | None:
    normalized_name = _normalize_text(source_geo_name)
    normalized_type = _normalize_text(source_geo_type)

    if normalized_type in {"national", "nation", "usa", "us", "country"}:
        return "us"

    if normalized_name in {"us", "usa", "united states", "united states of america"}:
        return "us"

    return None


def _lookup_geo_id(source_geo_name: str, source_geo_type: str) -> tuple[str, str, Decimal] | None:
    national_geo_id = _resolve_national_geo_id(source_geo_name, source_geo_type)

    if national_geo_id:
        return national_geo_id, "national_exact", Decimal("1.0000")

    normalized_name = _normalize_text(source_geo_name)
    normalized_type = _normalize_text(source_geo_type)

    if not normalized_name:
        return None

    if normalized_type in {"state", "states"}:
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
            geo_id = connection.execute(sql, {"name": normalized_name}).scalar_one_or_none()

        if geo_id:
            return str(geo_id), "state_name_exact", Decimal("0.9800")

    if normalized_type in {"metro", "msa", "cbsa", "metropolitan statistical area"}:
        like_name = f"%{normalized_name.split(' metro')[0]}%"

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
                    "name": normalized_name,
                    "like_name": like_name,
                },
            ).scalar_one_or_none()

        if geo_id:
            return str(geo_id), "metro_name_match", Decimal("0.8500")

    sql = text(
        """
        SELECT geo_id
        FROM geo.dim_geo
        WHERE lower(name) = :name
           OR lower(display_name) = :name
        ORDER BY geo_id
        LIMIT 1
        """
    )

    with engine.begin() as connection:
        geo_id = connection.execute(sql, {"name": normalized_name}).scalar_one_or_none()

    if geo_id:
        return str(geo_id), "geo_name_exact", Decimal("0.9000")

    return None


def map_records(raw_records: list[RawFhfaHpiRecord]) -> tuple[list[MappedFhfaHpiRecord], list[RawFhfaHpiRecord]]:
    mapped: list[MappedFhfaHpiRecord] = []
    unmatched: list[RawFhfaHpiRecord] = []

    cache: dict[tuple[str, str], tuple[str, str, Decimal] | None] = {}

    for record in raw_records:
        cache_key = (
            _normalize_text(record.source_geo_type),
            _normalize_text(record.source_geo_name),
        )

        if cache_key not in cache:
            cache[cache_key] = _lookup_geo_id(record.source_geo_name, record.source_geo_type)

        resolved = cache[cache_key]

        if resolved is None:
            unmatched.append(record)
            continue

        geo_id, match_method, confidence_score = resolved

        mapped.append(
            MappedFhfaHpiRecord(
                geo_id=geo_id,
                source_geo_name=record.source_geo_name,
                source_geo_type=record.source_geo_type,
                period=record.period,
                frequency=record.frequency,
                hpi=record.hpi,
                source_file_id=record.source_file_id,
                match_method=match_method,
                confidence_score=confidence_score,
            )
        )

    return mapped, unmatched


def _build_hpi_records(
    mapped_records: list[MappedFhfaHpiRecord],
    transform_run_id: str,
) -> list[MarketMetricRecord]:
    return [
        MarketMetricRecord(
            geo_id=record.geo_id,
            period_month=_period_month(record.period),
            metric_name="home_price_index",
            metric_value=_quantize_metric(record.hpi),
            metric_unit="index",
            source=SOURCE,
            dataset=DATASET,
            source_file_id=record.source_file_id,
            pipeline_run_id=transform_run_id,
            source_value=_quantize_metric(record.hpi),
            source_period=record.period,
            period_grain="monthly" if _normalize_text(record.frequency).startswith("month") else "quarterly",
            transformation_notes=(
                "Transformed FHFA HPI to canonical home_price_index."
            ),
            source_flags={
                "source_geo_name": record.source_geo_name,
                "source_geo_type": record.source_geo_type,
                "frequency": record.frequency,
                "match_method": record.match_method,
                "confidence_score": str(record.confidence_score),
            },
            quality_flags={
                "period_normalized_to_month_start": True,
                "source_frequency": record.frequency,
            },
        )
        for record in mapped_records
    ]


def _build_appreciation_records(
    mapped_records: list[MappedFhfaHpiRecord],
    transform_run_id: str,
) -> list[MarketMetricRecord]:
    grouped: dict[tuple[str, str], list[MappedFhfaHpiRecord]] = defaultdict(list)

    for record in mapped_records:
        grouped[(record.geo_id, _normalize_text(record.frequency))].append(record)

    records: list[MarketMetricRecord] = []

    for (_geo_id, frequency), group in grouped.items():
        sorted_group = sorted(group, key=lambda item: item.period)
        by_period = {record.period: record for record in sorted_group}

        if frequency.startswith("quarter"):
            previous_period_offset_months = 3
            yoy_offset_months = 12
            period_change_method = "quarter_over_quarter"
            period_grain = "quarterly"
        else:
            previous_period_offset_months = 1
            yoy_offset_months = 12
            period_change_method = "month_over_month"
            period_grain = "monthly"

        for record in sorted_group:
            current_period = record.period

            previous_period = _subtract_months(current_period, previous_period_offset_months)
            prior_year_period = _subtract_months(current_period, yoy_offset_months)

            previous_record = by_period.get(previous_period)
            prior_year_record = by_period.get(prior_year_period)

            if previous_record and previous_record.hpi != 0:
                period_change = ((record.hpi - previous_record.hpi) / previous_record.hpi) * Decimal("100")
                records.append(
                    MarketMetricRecord(
                        geo_id=record.geo_id,
                        period_month=_period_month(record.period),
                        metric_name="home_price_index_mom",
                        metric_value=_quantize_metric(period_change),
                        metric_unit="percent",
                        source=SOURCE,
                        dataset=DATASET,
                        source_file_id=record.source_file_id,
                        pipeline_run_id=transform_run_id,
                        source_value=_quantize_metric(period_change),
                        source_period=record.period,
                        period_grain=period_grain,
                        transformation_notes=(
                            f"Calculated FHFA HPI {period_change_method} appreciation."
                        ),
                        source_flags={
                            "source_geo_name": record.source_geo_name,
                            "frequency": record.frequency,
                            "calculation_method": period_change_method,
                            "current_hpi": str(record.hpi),
                            "prior_hpi": str(previous_record.hpi),
                        },
                        quality_flags={
                            "requires_prior_period": True,
                            "prior_period_available": True,
                        },
                    )
                )

            if prior_year_record and prior_year_record.hpi != 0:
                yoy = ((record.hpi - prior_year_record.hpi) / prior_year_record.hpi) * Decimal("100")
                records.append(
                    MarketMetricRecord(
                        geo_id=record.geo_id,
                        period_month=_period_month(record.period),
                        metric_name="home_price_index_yoy",
                        metric_value=_quantize_metric(yoy),
                        metric_unit="percent",
                        source=SOURCE,
                        dataset=DATASET,
                        source_file_id=record.source_file_id,
                        pipeline_run_id=transform_run_id,
                        source_value=_quantize_metric(yoy),
                        source_period=record.period,
                        period_grain=period_grain,
                        transformation_notes="Calculated FHFA HPI year-over-year appreciation.",
                        source_flags={
                            "source_geo_name": record.source_geo_name,
                            "frequency": record.frequency,
                            "calculation_method": "year_over_year",
                            "current_hpi": str(record.hpi),
                            "prior_year_hpi": str(prior_year_record.hpi),
                        },
                        quality_flags={
                            "requires_12_month_lookback": True,
                            "prior_year_available": True,
                        },
                    )
                )

    return records


def _subtract_months(value: date, months: int) -> date:
    total_months = value.year * 12 + value.month - 1 - months
    year = total_months // 12
    month = total_months % 12 + 1
    return date(year, month, 1)


def build_records(
    raw_records: list[RawFhfaHpiRecord],
    transform_run_id: str,
) -> tuple[list[MarketMetricRecord], list[RawFhfaHpiRecord]]:
    mapped_records, unmatched_records = map_records(raw_records)

    metric_records = [
        *_build_hpi_records(mapped_records, transform_run_id),
        *_build_appreciation_records(mapped_records, transform_run_id),
    ]

    return metric_records, unmatched_records


def main() -> None:
    transform_run_id = start_transform_run(
        transform_name=TRANSFORM_NAME,
        source=SOURCE,
        dataset=DATASET,
        target_table=TARGET_TABLE,
        metadata={
            "target_metrics": [
                "home_price_index",
                "home_price_index_yoy",
                "home_price_index_mom",
            ],
        },
    )

    try:
        raw_records = fetch_raw_fhfa_hpi()
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
            else f"{len(unmatched_records)} FHFA records had unmatched geography.",
        )

        print(
            f"FHFA HPI transform complete. "
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
