from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.transforms.common.market_metric_loader import upsert_market_metrics
from pipelines.transforms.common.market_metric_record import MarketMetricRecord
from pipelines.transforms.common.transform_audit import finish_transform_run, start_transform_run


SOURCE = "census"
DATASET = "building_permits"
TRANSFORM_NAME = "census_building_permits_transform"
TARGET_TABLE = "analytics.market_monthly_metrics"


PERMIT_METRICS: dict[str, tuple[str, str]] = {
    "building_permits": ("count", "building_permits"),
    "single_family_permits": ("count", "single_family_permits"),
    "multi_family_permits": ("count", "multi_family_permits"),
    "permit_units": ("count", "permit_units"),
}


@dataclass(frozen=True)
class RawCensusBpsRecord:
    geography_level: str
    period_type: str
    source_period_label: str
    source_geo_id: str
    source_name: str | None
    state_fips: str | None
    county_fips: str | None
    cbsa_code: str | None
    period_month: date
    building_permits: Decimal | None
    single_family_permits: Decimal | None
    multi_family_permits: Decimal | None
    permit_units: Decimal | None
    source_file_id: str | None


@dataclass(frozen=True)
class MappedCensusBpsRecord:
    geo_id: str
    geography_level: str
    period_type: str
    source_period_label: str
    source_geo_id: str
    source_name: str | None
    state_fips: str | None
    county_fips: str | None
    cbsa_code: str | None
    period_month: date
    building_permits: Decimal | None
    single_family_permits: Decimal | None
    multi_family_permits: Decimal | None
    permit_units: Decimal | None
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


def _quantize_count(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def fetch_raw_census_bps() -> list[RawCensusBpsRecord]:
    sql = text(
        """
        WITH ranked AS (
            SELECT
                geography_level,
                period_type,
                source_period_label,
                source_geo_id,
                source_name,
                state_fips,
                county_fips,
                cbsa_code,
                period_month,
                building_permits,
                single_family_permits,
                multi_family_permits,
                permit_units,
                source_file_id,
                ROW_NUMBER() OVER (
                    PARTITION BY geography_level, period_type, source_geo_id, period_month
                    ORDER BY load_date DESC, created_at DESC
                ) AS row_number
            FROM raw.census_building_permits
        )
        SELECT
            geography_level,
            period_type,
            source_period_label,
            source_geo_id,
            source_name,
            state_fips,
            county_fips,
            cbsa_code,
            period_month,
            building_permits,
            single_family_permits,
            multi_family_permits,
            permit_units,
            source_file_id
        FROM ranked
        WHERE row_number = 1
        ORDER BY geography_level, source_geo_id, period_month
        """
    )

    records: list[RawCensusBpsRecord] = []

    with engine.begin() as connection:
        result = connection.execute(sql)

        for row in result.mappings():
            records.append(
                RawCensusBpsRecord(
                    geography_level=row["geography_level"],
                    period_type=row["period_type"],
                    source_period_label=row["source_period_label"],
                    source_geo_id=row["source_geo_id"],
                    source_name=row["source_name"],
                    state_fips=row["state_fips"],
                    county_fips=row["county_fips"],
                    cbsa_code=row["cbsa_code"],
                    period_month=row["period_month"],
                    building_permits=_parse_decimal(row["building_permits"]),
                    single_family_permits=_parse_decimal(row["single_family_permits"]),
                    multi_family_permits=_parse_decimal(row["multi_family_permits"]),
                    permit_units=_parse_decimal(row["permit_units"]),
                    source_file_id=row["source_file_id"],
                )
            )

    return records


def _lookup_geo_id(record: RawCensusBpsRecord) -> tuple[str, str, Decimal] | None:
    geography_level = _normalize_text(record.geography_level)

    if geography_level == "national" or record.source_geo_id == "us":
        return "us", "national_exact", Decimal("1.0000")

    if geography_level == "state" and record.state_fips:
        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type = 'state'
              AND geo_id = :state_geo_id
            LIMIT 1
            """
        )

        with engine.begin() as connection:
            geo_id = connection.execute(
                sql,
                {
                    "state_geo_id": f"state:{record.state_fips.zfill(2)}",
                },
            ).scalar_one_or_none()

        if geo_id:
            return str(geo_id), "state_fips_exact", Decimal("1.0000")

    if geography_level == "county" and record.state_fips and record.county_fips:
        county_fips = f"{record.state_fips.zfill(2)}{record.county_fips.zfill(3)}"

        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type = 'county'
              AND geo_id = :county_geo_id
            LIMIT 1
            """
        )

        with engine.begin() as connection:
            geo_id = connection.execute(
                sql,
                {"county_geo_id": f"county:{county_fips}"},
            ).scalar_one_or_none()

        if geo_id:
            return str(geo_id), "county_fips_exact", Decimal("1.0000")

    if geography_level in {"cbsa", "metro"} and record.cbsa_code:
        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type IN ('metro', 'cbsa')
              AND cbsa_code = :cbsa_code
            LIMIT 1
            """
        )

        with engine.begin() as connection:
            geo_id = connection.execute(sql, {"cbsa_code": record.cbsa_code}).scalar_one_or_none()

        if geo_id:
            return str(geo_id), "cbsa_exact", Decimal("1.0000")

    return None


def map_records(
    raw_records: list[RawCensusBpsRecord],
) -> tuple[list[MappedCensusBpsRecord], list[RawCensusBpsRecord]]:
    mapped: list[MappedCensusBpsRecord] = []
    unmatched: list[RawCensusBpsRecord] = []

    for record in raw_records:
        resolved = _lookup_geo_id(record)

        if resolved is None:
            unmatched.append(record)
            continue

        geo_id, match_method, confidence_score = resolved

        mapped.append(
            MappedCensusBpsRecord(
                geo_id=geo_id,
                geography_level=record.geography_level,
                period_type=record.period_type,
                source_period_label=record.source_period_label,
                source_geo_id=record.source_geo_id,
                source_name=record.source_name,
                state_fips=record.state_fips,
                county_fips=record.county_fips,
                cbsa_code=record.cbsa_code,
                period_month=record.period_month,
                building_permits=record.building_permits,
                single_family_permits=record.single_family_permits,
                multi_family_permits=record.multi_family_permits,
                permit_units=record.permit_units,
                source_file_id=record.source_file_id,
                match_method=match_method,
                confidence_score=confidence_score,
            )
        )

    return mapped, unmatched


def build_records(
    raw_records: list[RawCensusBpsRecord],
    transform_run_id: str,
) -> tuple[list[MarketMetricRecord], list[RawCensusBpsRecord]]:
    mapped_records, unmatched_records = map_records(raw_records)
    metric_records: list[MarketMetricRecord] = []

    for record in mapped_records:
        for metric_name, (metric_unit, raw_column) in PERMIT_METRICS.items():
            value = getattr(record, raw_column)

            if value is None:
                continue

            normalized_value = _quantize_count(value)

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
                    period_grain="monthly" if record.period_type == "monthly" else "annual",
                    transformation_notes=f"Transformed Census BPS {metric_name}.",
                    source_flags={
                        "geography_level": record.geography_level,
                        "period_type": record.period_type,
                        "source_period_label": record.source_period_label,
                        "source_geo_id": record.source_geo_id,
                        "source_name": record.source_name,
                        "state_fips": record.state_fips,
                        "county_fips": record.county_fips,
                        "cbsa_code": record.cbsa_code,
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
            "target_metrics": sorted(PERMIT_METRICS),
        },
    )

    try:
        raw_records = fetch_raw_census_bps()
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
            else f"{len(unmatched_records)} Census BPS records had unmatched geography.",
        )

        print(
            f"Census BPS permits transform complete. "
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
