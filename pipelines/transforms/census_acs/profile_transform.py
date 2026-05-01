from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.transforms.common.market_metric_loader import upsert_market_metrics
from pipelines.transforms.common.market_metric_record import MarketMetricRecord
from pipelines.transforms.common.transform_audit import finish_transform_run, start_transform_run


SOURCE = "census_acs"
DATASET = "profile"
TRANSFORM_NAME = "census_acs_profile_transform"
TARGET_TABLE = "analytics.market_monthly_metrics"


ACS_METRICS: dict[str, tuple[str, str]] = {
    "population": ("count", "total_population"),
    "median_household_income": ("usd", "median_household_income"),
    "households": ("count", "occupied_housing_units"),
    "housing_units": ("count", "total_housing_units"),
    "occupied_housing_units": ("count", "occupied_housing_units"),
    "vacant_housing_units": ("count", "vacant_housing_units"),
    "owner_occupied_housing_units": ("count", "owner_occupied_housing_units"),
    "renter_occupied_housing_units": ("count", "renter_occupied_housing_units"),
    "median_gross_rent": ("usd", "median_gross_rent"),
    "rent_burden_pct": ("percent", "rent_burden_pct"),
}


@dataclass(frozen=True)
class RawCensusAcsProfileRecord:
    geography_level: str
    source_geo_id: str
    source_name: str
    state_fips: str | None
    county_fips: str | None
    cbsa_code: str | None
    year: int
    source_period_start: date
    source_period_end: date
    total_population: Decimal | None
    median_household_income: Decimal | None
    total_housing_units: Decimal | None
    occupied_housing_units: Decimal | None
    vacant_housing_units: Decimal | None
    owner_occupied_housing_units: Decimal | None
    renter_occupied_housing_units: Decimal | None
    median_gross_rent: Decimal | None
    rent_burden_pct: Decimal | None
    source_file_id: str | None


@dataclass(frozen=True)
class MappedCensusAcsProfileRecord:
    geo_id: str
    geography_level: str
    source_geo_id: str
    source_name: str
    state_fips: str | None
    county_fips: str | None
    cbsa_code: str | None
    year: int
    source_period_start: date
    source_period_end: date
    total_population: Decimal | None
    median_household_income: Decimal | None
    total_housing_units: Decimal | None
    occupied_housing_units: Decimal | None
    vacant_housing_units: Decimal | None
    owner_occupied_housing_units: Decimal | None
    renter_occupied_housing_units: Decimal | None
    median_gross_rent: Decimal | None
    rent_burden_pct: Decimal | None
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


def _period_month_for_year(year: int) -> date:
    return date(year, 12, 1)


def _quantize_count(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _quantize_pct(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def _metric_value(record: MappedCensusAcsProfileRecord, raw_column: str) -> Decimal | None:
    return getattr(record, raw_column)


def fetch_raw_census_acs_profile() -> list[RawCensusAcsProfileRecord]:
    sql = text(
        """
        WITH ranked AS (
            SELECT
                geography_level,
                source_geo_id,
                source_name,
                state_fips,
                county_fips,
                cbsa_code,
                year,
                source_period_start,
                source_period_end,
                total_population,
                median_household_income,
                total_housing_units,
                occupied_housing_units,
                vacant_housing_units,
                owner_occupied_housing_units,
                renter_occupied_housing_units,
                median_gross_rent,
                rent_burden_pct,
                source_file_id,
                ROW_NUMBER() OVER (
                    PARTITION BY geography_level, source_geo_id, year
                    ORDER BY load_date DESC, created_at DESC
                ) AS row_number
            FROM raw.census_acs_profile
        )
        SELECT
            geography_level,
            source_geo_id,
            source_name,
            state_fips,
            county_fips,
            cbsa_code,
            year,
            source_period_start,
            source_period_end,
            total_population,
            median_household_income,
            total_housing_units,
            occupied_housing_units,
            vacant_housing_units,
            owner_occupied_housing_units,
            renter_occupied_housing_units,
            median_gross_rent,
            rent_burden_pct,
            source_file_id
        FROM ranked
        WHERE row_number = 1
        ORDER BY geography_level, source_geo_id, year
        """
    )

    records: list[RawCensusAcsProfileRecord] = []

    with engine.begin() as connection:
        result = connection.execute(sql)

        for row in result.mappings():
            records.append(
                RawCensusAcsProfileRecord(
                    geography_level=row["geography_level"],
                    source_geo_id=row["source_geo_id"],
                    source_name=row["source_name"],
                    state_fips=row["state_fips"],
                    county_fips=row["county_fips"],
                    cbsa_code=row["cbsa_code"],
                    year=int(row["year"]),
                    source_period_start=row["source_period_start"],
                    source_period_end=row["source_period_end"],
                    total_population=_parse_decimal(row["total_population"]),
                    median_household_income=_parse_decimal(row["median_household_income"]),
                    total_housing_units=_parse_decimal(row["total_housing_units"]),
                    occupied_housing_units=_parse_decimal(row["occupied_housing_units"]),
                    vacant_housing_units=_parse_decimal(row["vacant_housing_units"]),
                    owner_occupied_housing_units=_parse_decimal(
                        row["owner_occupied_housing_units"]
                    ),
                    renter_occupied_housing_units=_parse_decimal(
                        row["renter_occupied_housing_units"]
                    ),
                    median_gross_rent=_parse_decimal(row["median_gross_rent"]),
                    rent_burden_pct=_parse_decimal(row["rent_burden_pct"]),
                    source_file_id=row["source_file_id"],
                )
            )

    return records


def _lookup_geo_id(record: RawCensusAcsProfileRecord) -> tuple[str, str, Decimal] | None:
    if record.geography_level == "state" and record.state_fips:
        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type = 'state'
              AND geo_id = :geo_id
            LIMIT 1
            """
        )

        geo_id = f"state:{record.state_fips}"

        with engine.begin() as connection:
            found = connection.execute(sql, {"geo_id": geo_id}).scalar_one_or_none()

        if found:
            return str(found), "state_fips_exact", Decimal("1.0000")

    if record.geography_level == "county" and record.state_fips and record.county_fips:
        county_fips = f"{record.state_fips}{record.county_fips}"
        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type = 'county'
              AND county_fips = :county_fips
            LIMIT 1
            """
        )

        with engine.begin() as connection:
            found = connection.execute(sql, {"county_fips": county_fips}).scalar_one_or_none()

        if found:
            return str(found), "county_fips_exact", Decimal("1.0000")

    if record.geography_level == "metro" and record.cbsa_code:
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
            found = connection.execute(sql, {"cbsa_code": record.cbsa_code}).scalar_one_or_none()

        if found:
            return str(found), "cbsa_exact", Decimal("1.0000")

    return None


def map_records(
    raw_records: list[RawCensusAcsProfileRecord],
) -> tuple[list[MappedCensusAcsProfileRecord], list[RawCensusAcsProfileRecord]]:
    mapped: list[MappedCensusAcsProfileRecord] = []
    unmatched: list[RawCensusAcsProfileRecord] = []

    for record in raw_records:
        resolved = _lookup_geo_id(record)

        if resolved is None:
            unmatched.append(record)
            continue

        geo_id, match_method, confidence_score = resolved

        mapped.append(
            MappedCensusAcsProfileRecord(
                geo_id=geo_id,
                geography_level=record.geography_level,
                source_geo_id=record.source_geo_id,
                source_name=record.source_name,
                state_fips=record.state_fips,
                county_fips=record.county_fips,
                cbsa_code=record.cbsa_code,
                year=record.year,
                source_period_start=record.source_period_start,
                source_period_end=record.source_period_end,
                total_population=record.total_population,
                median_household_income=record.median_household_income,
                total_housing_units=record.total_housing_units,
                occupied_housing_units=record.occupied_housing_units,
                vacant_housing_units=record.vacant_housing_units,
                owner_occupied_housing_units=record.owner_occupied_housing_units,
                renter_occupied_housing_units=record.renter_occupied_housing_units,
                median_gross_rent=record.median_gross_rent,
                rent_burden_pct=record.rent_burden_pct,
                source_file_id=record.source_file_id,
                match_method=match_method,
                confidence_score=confidence_score,
            )
        )

    return mapped, unmatched


def _share(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if numerator is None or denominator is None or denominator == 0:
        return None

    return (numerator / denominator) * Decimal("100")


def build_records(
    raw_records: list[RawCensusAcsProfileRecord],
    transform_run_id: str,
) -> tuple[list[MarketMetricRecord], list[RawCensusAcsProfileRecord]]:
    mapped_records, unmatched_records = map_records(raw_records)
    metric_records: list[MarketMetricRecord] = []

    for record in mapped_records:
        metric_values: dict[str, tuple[Decimal | None, str]] = {
            **{
                metric_name: (_metric_value(record, raw_column), unit)
                for metric_name, (unit, raw_column) in ACS_METRICS.items()
            },
            "owner_occupied_share": (
                _share(record.owner_occupied_housing_units, record.occupied_housing_units),
                "percent",
            ),
            "renter_occupied_share": (
                _share(record.renter_occupied_housing_units, record.occupied_housing_units),
                "percent",
            ),
        }

        for metric_name, (value, unit) in metric_values.items():
            if value is None:
                continue

            if unit == "usd":
                normalized_value = _quantize_money(value)
            elif unit == "percent":
                normalized_value = _quantize_pct(value)
            else:
                normalized_value = _quantize_count(value)

            metric_records.append(
                MarketMetricRecord(
                    geo_id=record.geo_id,
                    period_month=_period_month_for_year(record.year),
                    metric_name=metric_name,
                    metric_value=normalized_value,
                    metric_unit=unit,
                    source=SOURCE,
                    dataset=DATASET,
                    source_file_id=record.source_file_id,
                    pipeline_run_id=transform_run_id,
                    source_value=normalized_value,
                    source_period=record.source_period_end,
                    period_grain="annual",
                    transformation_notes=f"Transformed Census ACS profile {metric_name}.",
                    source_flags={
                        "geography_level": record.geography_level,
                        "source_geo_id": record.source_geo_id,
                        "source_name": record.source_name,
                        "state_fips": record.state_fips,
                        "county_fips": record.county_fips,
                        "cbsa_code": record.cbsa_code,
                        "year": record.year,
                        "source_period_start": str(record.source_period_start),
                        "source_period_end": str(record.source_period_end),
                        "match_method": record.match_method,
                        "confidence_score": str(record.confidence_score),
                    },
                    quality_flags={
                        "acs_5_year_estimate": True,
                        "period_month_represents_acs_year_end": True,
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
            "target_metrics": [
                *sorted(ACS_METRICS),
                "owner_occupied_share",
                "renter_occupied_share",
            ],
        },
    )

    try:
        raw_records = fetch_raw_census_acs_profile()
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
            else f"{len(unmatched_records)} Census ACS records had unmatched geography.",
        )

        print(
            f"Census ACS profile transform complete. "
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
