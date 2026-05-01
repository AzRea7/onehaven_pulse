from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.transforms.common.market_metric_loader import upsert_market_metrics
from pipelines.transforms.common.market_metric_record import MarketMetricRecord
from pipelines.transforms.common.transform_audit import finish_transform_run, start_transform_run


SOURCE = "redfin"
DATASET = "market_tracker"
TRANSFORM_NAME = "redfin_market_tracker_transform"
TARGET_TABLE = "analytics.market_monthly_metrics"


REDFIN_METRICS: dict[str, tuple[str, str]] = {
    "median_sale_price": ("usd", "median_sale_price"),
    "homes_sold": ("count", "homes_sold"),
    "pending_sales": ("count", "pending_sales"),
    "new_listings": ("count", "new_listings"),
    "active_listings": ("count", "active_listings"),
    "months_supply": ("months", "months_supply"),
    "median_days_on_market": ("days", "median_days_on_market"),
    "sale_to_list_ratio": ("ratio", "sale_to_list_ratio"),
    "price_drops_pct": ("percent", "price_drops_pct"),
}


@dataclass(frozen=True)
class RawRedfinRecord:
    source_region_id: str | None
    region_name: str
    region_type: str | None
    state_code: str | None
    property_type: str | None
    period_month: date
    median_sale_price: Decimal | None
    homes_sold: Decimal | None
    pending_sales: Decimal | None
    new_listings: Decimal | None
    active_listings: Decimal | None
    months_supply: Decimal | None
    median_days_on_market: Decimal | None
    sale_to_list_ratio: Decimal | None
    price_drops_pct: Decimal | None
    source_file_id: str | None


@dataclass(frozen=True)
class MappedRedfinRecord:
    geo_id: str
    source_region_id: str | None
    region_name: str
    region_type: str | None
    state_code: str | None
    property_type: str | None
    period_month: date
    median_sale_price: Decimal | None
    homes_sold: Decimal | None
    pending_sales: Decimal | None
    new_listings: Decimal | None
    active_listings: Decimal | None
    months_supply: Decimal | None
    median_days_on_market: Decimal | None
    sale_to_list_ratio: Decimal | None
    price_drops_pct: Decimal | None
    source_file_id: str | None
    match_method: str
    confidence_score: Decimal


def _normalize_text(value: str | None) -> str:
    return " ".join((value or "").strip().lower().replace(",", "").split())


def _parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None

    raw = str(value).strip()

    if not raw or raw == ".":
        return None

    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _quantize_metric(value: Decimal, metric_name: str) -> Decimal:
    if metric_name in {"median_sale_price"}:
        return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    if metric_name in {
        "homes_sold",
        "pending_sales",
        "new_listings",
        "active_listings",
    }:
        return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def fetch_raw_redfin_records() -> list[RawRedfinRecord]:
    sql = text(
        """
        WITH ranked AS (
            SELECT
                source_region_id,
                region_name,
                region_type,
                state_code,
                property_type,
                period_month,
                median_sale_price,
                homes_sold,
                pending_sales,
                new_listings,
                active_listings,
                months_supply,
                median_days_on_market,
                sale_to_list_ratio,
                price_drops_pct,
                source_file_id,
                ROW_NUMBER() OVER (
                    PARTITION BY region_name, region_type, property_type, period_month
                    ORDER BY load_date DESC, created_at DESC
                ) AS row_number
            FROM raw.redfin_market_tracker
        )
        SELECT
            source_region_id,
            region_name,
            region_type,
            state_code,
            property_type,
            period_month,
            median_sale_price,
            homes_sold,
            pending_sales,
            new_listings,
            active_listings,
            months_supply,
            median_days_on_market,
            sale_to_list_ratio,
            price_drops_pct,
            source_file_id
        FROM ranked
        WHERE row_number = 1
        ORDER BY region_name, period_month
        """
    )

    records: list[RawRedfinRecord] = []

    with engine.begin() as connection:
        result = connection.execute(sql)

        for row in result.mappings():
            records.append(
                RawRedfinRecord(
                    source_region_id=row["source_region_id"],
                    region_name=row["region_name"],
                    region_type=row["region_type"],
                    state_code=row["state_code"],
                    property_type=row["property_type"],
                    period_month=row["period_month"],
                    median_sale_price=_parse_decimal(row["median_sale_price"]),
                    homes_sold=_parse_decimal(row["homes_sold"]),
                    pending_sales=_parse_decimal(row["pending_sales"]),
                    new_listings=_parse_decimal(row["new_listings"]),
                    active_listings=_parse_decimal(row["active_listings"]),
                    months_supply=_parse_decimal(row["months_supply"]),
                    median_days_on_market=_parse_decimal(row["median_days_on_market"]),
                    sale_to_list_ratio=_parse_decimal(row["sale_to_list_ratio"]),
                    price_drops_pct=_parse_decimal(row["price_drops_pct"]),
                    source_file_id=row["source_file_id"],
                )
            )

    return records



def _slugify(value: str) -> str:
    normalized = _normalize_text(value)
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = normalized.strip("_")
    return normalized or "unknown"


def _provisional_redfin_geo_id(record: RawRedfinRecord) -> str:
    state = _normalize_text(record.state_code).replace(" ", "_")
    region = _slugify(record.region_name)

    if state:
        return f"metro_redfin_{region}_{state}"

    return f"metro_redfin_{region}"


def _is_metro_like(record: RawRedfinRecord) -> bool:
    region_type = _normalize_text(record.region_type)
    region_name = _normalize_text(record.region_name)

    return (
        region_type in {"metro", "msa", "cbsa"}
        or "metro" in region_name
        or "," in record.region_name
    )


def _ensure_provisional_redfin_geo(record: RawRedfinRecord) -> str:
    geo_id = _provisional_redfin_geo_id(record)

    sql = text(
        """
        INSERT INTO geo.dim_geo (
            geo_id,
            geo_type,
            name,
            display_name,
            state_code,
            country_code,
            is_active,
            created_at,
            updated_at
        )
        VALUES (
            :geo_id,
            'metro',
            :name,
            :display_name,
            :state_code,
            'US',
            true,
            now(),
            now()
        )
        ON CONFLICT (geo_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            display_name = EXCLUDED.display_name,
            state_code = EXCLUDED.state_code,
            updated_at = now()
        """
    )

    with engine.begin() as connection:
        connection.execute(
            sql,
            {
                "geo_id": geo_id,
                "name": record.region_name,
                "display_name": record.region_name,
                "state_code": record.state_code,
            },
        )

    return geo_id

def _lookup_geo_id(record: RawRedfinRecord) -> tuple[str, str, Decimal] | None:
    region_name = _normalize_text(record.region_name)
    region_type = _normalize_text(record.region_type)

    if region_name in {"united states", "us", "usa"}:
        return "us", "redfin_country", Decimal("1.0000")

    if region_type in {"national", "country"}:
        return "us", "redfin_country", Decimal("1.0000")

    if region_type in {"state"}:
        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type = 'state'
              AND (
                    lower(name) = :name
                 OR lower(display_name) = :name
                 OR lower(state_code) = :state_code
                 OR lower(state_name) = :name
              )
            ORDER BY geo_id
            LIMIT 1
            """
        )

        with engine.begin() as connection:
            geo_id = connection.execute(
                sql,
                {
                    "name": region_name,
                    "state_code": _normalize_text(record.state_code),
                },
            ).scalar_one_or_none()

        if geo_id:
            return str(geo_id), "state_exact", Decimal("0.9800")

    if region_type in {"metro", "msa", "cbsa"}:
        metro_name = region_name
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

    if _is_metro_like(record):
        geo_id = _ensure_provisional_redfin_geo(record)
        return geo_id, "redfin_provisional_metro", Decimal("0.6000")

    return None


def map_records(
    raw_records: list[RawRedfinRecord],
) -> tuple[list[MappedRedfinRecord], list[RawRedfinRecord]]:
    mapped: list[MappedRedfinRecord] = []
    unmatched: list[RawRedfinRecord] = []

    cache: dict[tuple[str, str, str | None], tuple[str, str, Decimal] | None] = {}

    for record in raw_records:
        key = (
            _normalize_text(record.region_type),
            _normalize_text(record.region_name),
            record.state_code,
        )

        if key not in cache:
            cache[key] = _lookup_geo_id(record)

        resolved = cache[key]

        if resolved is None:
            unmatched.append(record)
            continue

        geo_id, match_method, confidence_score = resolved

        mapped.append(
            MappedRedfinRecord(
                geo_id=geo_id,
                source_region_id=record.source_region_id,
                region_name=record.region_name,
                region_type=record.region_type,
                state_code=record.state_code,
                property_type=record.property_type,
                period_month=record.period_month,
                median_sale_price=record.median_sale_price,
                homes_sold=record.homes_sold,
                pending_sales=record.pending_sales,
                new_listings=record.new_listings,
                active_listings=record.active_listings,
                months_supply=record.months_supply,
                median_days_on_market=record.median_days_on_market,
                sale_to_list_ratio=record.sale_to_list_ratio,
                price_drops_pct=record.price_drops_pct,
                source_file_id=record.source_file_id,
                match_method=match_method,
                confidence_score=confidence_score,
            )
        )

    return mapped, unmatched


def _metric_value(record: MappedRedfinRecord, raw_column: str) -> Decimal | None:
    return getattr(record, raw_column)


def build_records(
    raw_records: list[RawRedfinRecord],
    transform_run_id: str,
) -> tuple[list[MarketMetricRecord], list[RawRedfinRecord]]:
    mapped_records, unmatched_records = map_records(raw_records)
    metric_records: list[MarketMetricRecord] = []

    for record in mapped_records:
        for metric_name, (unit, raw_column) in REDFIN_METRICS.items():
            value = _metric_value(record, raw_column)

            if value is None:
                continue

            metric_records.append(
                MarketMetricRecord(
                    geo_id=record.geo_id,
                    period_month=record.period_month,
                    metric_name=metric_name,
                    metric_value=_quantize_metric(value, metric_name),
                    metric_unit=unit,
                    source=SOURCE,
                    dataset=DATASET,
                    source_file_id=record.source_file_id,
                    pipeline_run_id=transform_run_id,
                    source_value=_quantize_metric(value, metric_name),
                    source_period=record.period_month,
                    period_grain="monthly",
                    transformation_notes=f"Transformed Redfin {metric_name}.",
                    source_flags={
                        "source_region_id": record.source_region_id,
                        "region_name": record.region_name,
                        "region_type": record.region_type,
                        "state_code": record.state_code,
                        "property_type": record.property_type,
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
            "target_metrics": sorted(REDFIN_METRICS),
        },
    )

    try:
        raw_records = fetch_raw_redfin_records()
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
            else f"{len(unmatched_records)} Redfin records had unmatched geography.",
        )

        print(
            f"Redfin market tracker transform complete. "
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
