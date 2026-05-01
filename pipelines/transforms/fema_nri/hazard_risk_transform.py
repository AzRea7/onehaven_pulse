from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.transforms.common.market_metric_loader import upsert_market_metrics
from pipelines.transforms.common.market_metric_record import MarketMetricRecord
from pipelines.transforms.common.transform_audit import finish_transform_run, start_transform_run


SOURCE = "fema_nri"
DATASET = "county_risk"
TRANSFORM_NAME = "fema_nri_hazard_risk_transform"
TARGET_TABLE = "analytics.market_monthly_metrics"


FEMA_NRI_METRICS: dict[str, tuple[str, str]] = {
    "hazard_risk_score": ("score", "risk_score"),
    "expected_annual_loss": ("usd", "expected_annual_loss"),
    "social_vulnerability_score": ("score", "social_vulnerability_score"),
    "community_resilience_score": ("score", "community_resilience_score"),
}


@dataclass(frozen=True)
class RawFemaNriCountyRiskRecord:
    county_fips: str
    county_name: str | None
    state_name: str | None
    state_code: str | None
    release_label: str | None
    risk_score: Decimal | None
    risk_rating: str | None
    expected_annual_loss: Decimal | None
    expected_annual_loss_score: Decimal | None
    expected_annual_loss_rating: str | None
    social_vulnerability_score: Decimal | None
    social_vulnerability_rating: str | None
    community_resilience_score: Decimal | None
    community_resilience_rating: str | None
    source_file_id: str | None


@dataclass(frozen=True)
class MappedFemaNriCountyRiskRecord:
    geo_id: str
    county_fips: str
    county_name: str | None
    state_name: str | None
    state_code: str | None
    release_label: str | None
    risk_score: Decimal | None
    risk_rating: str | None
    expected_annual_loss: Decimal | None
    expected_annual_loss_score: Decimal | None
    expected_annual_loss_rating: str | None
    social_vulnerability_score: Decimal | None
    social_vulnerability_rating: str | None
    community_resilience_score: Decimal | None
    community_resilience_rating: str | None
    source_file_id: str | None
    match_method: str
    confidence_score: Decimal


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


def _quantize_score(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _period_month_for_release(_release_label: str | None) -> date:
    # FEMA NRI is periodic, not monthly. Store at current known release year-end.
    # Keep this stable for deterministic transforms.
    return date(2025, 12, 1)


def fetch_raw_fema_nri_county_risk() -> list[RawFemaNriCountyRiskRecord]:
    sql = text(
        """
        WITH ranked AS (
            SELECT
                county_fips,
                county_name,
                state_name,
                state_code,
                release_label,
                risk_score,
                risk_rating,
                expected_annual_loss,
                expected_annual_loss_score,
                expected_annual_loss_rating,
                social_vulnerability_score,
                social_vulnerability_rating,
                community_resilience_score,
                community_resilience_rating,
                source_file_id,
                ROW_NUMBER() OVER (
                    PARTITION BY county_fips, release_label
                    ORDER BY load_date DESC, created_at DESC
                ) AS row_number
            FROM raw.fema_nri_county_risk
        )
        SELECT
            county_fips,
            county_name,
            state_name,
            state_code,
            release_label,
            risk_score,
            risk_rating,
            expected_annual_loss,
            expected_annual_loss_score,
            expected_annual_loss_rating,
            social_vulnerability_score,
            social_vulnerability_rating,
            community_resilience_score,
            community_resilience_rating,
            source_file_id
        FROM ranked
        WHERE row_number = 1
        ORDER BY county_fips
        """
    )

    records: list[RawFemaNriCountyRiskRecord] = []

    with engine.begin() as connection:
        result = connection.execute(sql)

        for row in result.mappings():
            records.append(
                RawFemaNriCountyRiskRecord(
                    county_fips=row["county_fips"],
                    county_name=row["county_name"],
                    state_name=row["state_name"],
                    state_code=row["state_code"],
                    release_label=row["release_label"],
                    risk_score=_parse_decimal(row["risk_score"]),
                    risk_rating=row["risk_rating"],
                    expected_annual_loss=_parse_decimal(row["expected_annual_loss"]),
                    expected_annual_loss_score=_parse_decimal(
                        row["expected_annual_loss_score"]
                    ),
                    expected_annual_loss_rating=row["expected_annual_loss_rating"],
                    social_vulnerability_score=_parse_decimal(
                        row["social_vulnerability_score"]
                    ),
                    social_vulnerability_rating=row["social_vulnerability_rating"],
                    community_resilience_score=_parse_decimal(
                        row["community_resilience_score"]
                    ),
                    community_resilience_rating=row["community_resilience_rating"],
                    source_file_id=row["source_file_id"],
                )
            )

    return records


def _ensure_provisional_county_geo(record: RawFemaNriCountyRiskRecord) -> str:
    geo_id = f"county:{record.county_fips}"

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
            'county',
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

    name = record.county_name or record.county_fips
    display_name = (
        f"{record.county_name}, {record.state_code}"
        if record.county_name and record.state_code
        else name
    )

    with engine.begin() as connection:
        connection.execute(
            sql,
            {
                "geo_id": geo_id,
                "name": name,
                "display_name": display_name,
                "state_code": record.state_code,
            },
        )

    return geo_id


def _lookup_geo_id(record: RawFemaNriCountyRiskRecord) -> tuple[str, str, Decimal]:
    geo_id = f"county:{record.county_fips}"

    sql = text(
        """
        SELECT geo_id
        FROM geo.dim_geo
        WHERE geo_id = :geo_id
        LIMIT 1
        """
    )

    with engine.begin() as connection:
        found = connection.execute(sql, {"geo_id": geo_id}).scalar_one_or_none()

    if found:
        return str(found), "county_fips_exact", Decimal("1.0000")

    provisional_geo_id = _ensure_provisional_county_geo(record)
    return provisional_geo_id, "fema_nri_provisional_county", Decimal("0.6000")


def map_records(
    raw_records: list[RawFemaNriCountyRiskRecord],
) -> tuple[list[MappedFemaNriCountyRiskRecord], list[RawFemaNriCountyRiskRecord]]:
    mapped: list[MappedFemaNriCountyRiskRecord] = []

    for record in raw_records:
        geo_id, match_method, confidence_score = _lookup_geo_id(record)

        mapped.append(
            MappedFemaNriCountyRiskRecord(
                geo_id=geo_id,
                county_fips=record.county_fips,
                county_name=record.county_name,
                state_name=record.state_name,
                state_code=record.state_code,
                release_label=record.release_label,
                risk_score=record.risk_score,
                risk_rating=record.risk_rating,
                expected_annual_loss=record.expected_annual_loss,
                expected_annual_loss_score=record.expected_annual_loss_score,
                expected_annual_loss_rating=record.expected_annual_loss_rating,
                social_vulnerability_score=record.social_vulnerability_score,
                social_vulnerability_rating=record.social_vulnerability_rating,
                community_resilience_score=record.community_resilience_score,
                community_resilience_rating=record.community_resilience_rating,
                source_file_id=record.source_file_id,
                match_method=match_method,
                confidence_score=confidence_score,
            )
        )

    return mapped, []


def build_records(
    raw_records: list[RawFemaNriCountyRiskRecord],
    transform_run_id: str,
) -> tuple[list[MarketMetricRecord], list[RawFemaNriCountyRiskRecord]]:
    mapped_records, unmatched_records = map_records(raw_records)
    metric_records: list[MarketMetricRecord] = []

    for record in mapped_records:
        period_month = _period_month_for_release(record.release_label)

        for metric_name, (metric_unit, raw_column) in FEMA_NRI_METRICS.items():
            value = getattr(record, raw_column)

            if value is None:
                continue

            normalized_value = (
                _quantize_money(value)
                if metric_unit == "usd"
                else _quantize_score(value)
            )

            metric_records.append(
                MarketMetricRecord(
                    geo_id=record.geo_id,
                    period_month=period_month,
                    metric_name=metric_name,
                    metric_value=normalized_value,
                    metric_unit=metric_unit,
                    source=SOURCE,
                    dataset=DATASET,
                    source_file_id=record.source_file_id,
                    pipeline_run_id=transform_run_id,
                    source_value=normalized_value,
                    source_period=period_month,
                    period_grain="annual",
                    transformation_notes=f"Transformed FEMA NRI {metric_name}.",
                    source_flags={
                        "county_fips": record.county_fips,
                        "county_name": record.county_name,
                        "state_name": record.state_name,
                        "state_code": record.state_code,
                        "release_label": record.release_label,
                        "risk_rating": record.risk_rating,
                        "expected_annual_loss_score": str(record.expected_annual_loss_score)
                        if record.expected_annual_loss_score is not None
                        else None,
                        "expected_annual_loss_rating": record.expected_annual_loss_rating,
                        "social_vulnerability_rating": record.social_vulnerability_rating,
                        "community_resilience_rating": record.community_resilience_rating,
                        "match_method": record.match_method,
                        "confidence_score": str(record.confidence_score),
                    },
                    quality_flags={
                        "period_represents_nri_release": True,
                        "county_level_metric": True,
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
            "target_metrics": sorted(FEMA_NRI_METRICS),
        },
    )

    try:
        raw_records = fetch_raw_fema_nri_county_risk()
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
            else f"{len(unmatched_records)} FEMA NRI records had unmatched geography.",
        )

        print(
            f"FEMA NRI hazard risk transform complete. "
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
