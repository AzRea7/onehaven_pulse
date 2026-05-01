from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.transforms.common.market_metric_loader import upsert_market_metrics
from pipelines.transforms.common.market_metric_record import MarketMetricRecord
from pipelines.transforms.common.transform_audit import finish_transform_run, start_transform_run


SOURCE = "derived"
DATASET = "market_ratios"
TRANSFORM_NAME = "derived_market_ratios_transform"
TARGET_TABLE = "analytics.market_monthly_metrics"

LOAN_TO_VALUE = Decimal("0.80")
MORTGAGE_TERM_MONTHS = Decimal("360")


@dataclass(frozen=True)
class CanonicalMarketSnapshot:
    geo_id: str
    period_month: date
    median_sale_price: Decimal | None
    zhvi: Decimal | None
    home_price_index: Decimal | None
    cpi: Decimal | None
    median_household_income: Decimal | None
    median_rent: Decimal | None
    zori: Decimal | None
    mortgage_rate_30y: Decimal | None
    permit_units: Decimal | None
    building_permits: Decimal | None
    population: Decimal | None


@dataclass(frozen=True)
class DerivedMetric:
    metric_name: str
    metric_value: Decimal
    metric_unit: str
    transformation_notes: str
    quality_flags: dict


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


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _ratio(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def _positive(value: Decimal | None) -> bool:
    return value is not None and value > 0


def _home_price(snapshot: CanonicalMarketSnapshot) -> tuple[Decimal | None, str | None]:
    if _positive(snapshot.median_sale_price):
        return snapshot.median_sale_price, "median_sale_price"

    if _positive(snapshot.zhvi):
        return snapshot.zhvi, "zhvi"

    return None, None


def _monthly_rent(snapshot: CanonicalMarketSnapshot) -> tuple[Decimal | None, str | None]:
    if _positive(snapshot.median_rent):
        return snapshot.median_rent, "median_rent"

    if _positive(snapshot.zori):
        return snapshot.zori, "zori"

    return None, None


def _monthly_payment(
    *,
    home_price: Decimal,
    annual_rate_percent: Decimal,
) -> Decimal | None:
    loan_amount = home_price * LOAN_TO_VALUE
    monthly_rate = annual_rate_percent / Decimal("100") / Decimal("12")

    if monthly_rate <= 0:
        return loan_amount / MORTGAGE_TERM_MONTHS

    numerator = monthly_rate * (Decimal("1") + monthly_rate) ** int(MORTGAGE_TERM_MONTHS)
    denominator = (Decimal("1") + monthly_rate) ** int(MORTGAGE_TERM_MONTHS) - Decimal("1")

    if denominator == 0:
        return None

    return loan_amount * numerator / denominator


def fetch_canonical_snapshots() -> list[CanonicalMarketSnapshot]:
    sql = text(
        """
        SELECT
            geo_id,
            period_month,
            median_sale_price,
            zhvi,
            home_price_index,
            cpi,
            median_household_income,
            median_rent,
            zori,
            mortgage_rate_30y,
            permit_units,
            building_permits,
            population
        FROM analytics.market_monthly_metrics
        WHERE median_sale_price IS NOT NULL
           OR zhvi IS NOT NULL
           OR home_price_index IS NOT NULL
           OR median_household_income IS NOT NULL
           OR median_rent IS NOT NULL
           OR zori IS NOT NULL
           OR mortgage_rate_30y IS NOT NULL
           OR permit_units IS NOT NULL
           OR building_permits IS NOT NULL
           OR population IS NOT NULL
        ORDER BY geo_id, period_month
        """
    )

    snapshots: list[CanonicalMarketSnapshot] = []

    with engine.begin() as connection:
        result = connection.execute(sql)

        for row in result.mappings():
            snapshots.append(
                CanonicalMarketSnapshot(
                    geo_id=row["geo_id"],
                    period_month=row["period_month"],
                    median_sale_price=_parse_decimal(row["median_sale_price"]),
                    zhvi=_parse_decimal(row["zhvi"]),
                    home_price_index=_parse_decimal(row["home_price_index"]),
                    cpi=_parse_decimal(row["cpi"]),
                    median_household_income=_parse_decimal(row["median_household_income"]),
                    median_rent=_parse_decimal(row["median_rent"]),
                    zori=_parse_decimal(row["zori"]),
                    mortgage_rate_30y=_parse_decimal(row["mortgage_rate_30y"]),
                    permit_units=_parse_decimal(row["permit_units"]),
                    building_permits=_parse_decimal(row["building_permits"]),
                    population=_parse_decimal(row["population"]),
                )
            )

    return snapshots


def derive_snapshot_metrics(snapshot: CanonicalMarketSnapshot) -> list[DerivedMetric]:
    metrics: list[DerivedMetric] = []

    home_price, home_price_source = _home_price(snapshot)
    monthly_rent, rent_source = _monthly_rent(snapshot)

    if _positive(home_price) and _positive(snapshot.median_household_income):
        price_to_income = home_price / snapshot.median_household_income

        metrics.append(
            DerivedMetric(
                metric_name="price_to_income_ratio",
                metric_value=_ratio(price_to_income),
                metric_unit="ratio",
                transformation_notes=(
                    f"Derived as {home_price_source} / median_household_income."
                ),
                quality_flags={
                    "home_price_source": home_price_source,
                    "requires_income": True,
                },
            )
        )

    monthly_payment = None

    if _positive(home_price) and _positive(snapshot.mortgage_rate_30y):
        monthly_payment = _monthly_payment(
            home_price=home_price,
            annual_rate_percent=snapshot.mortgage_rate_30y,
        )

        if monthly_payment is not None:
            metrics.append(
                DerivedMetric(
                    metric_name="estimated_monthly_payment",
                    metric_value=_money(monthly_payment),
                    metric_unit="usd",
                    transformation_notes=(
                        "Derived from home price, 80 percent LTV, 30-year fixed "
                        "amortization, and mortgage_rate_30y."
                    ),
                    quality_flags={
                        "home_price_source": home_price_source,
                        "loan_to_value": str(LOAN_TO_VALUE),
                        "term_months": int(MORTGAGE_TERM_MONTHS),
                    },
                )
            )

    if (
        monthly_payment is not None
        and _positive(snapshot.median_household_income)
        and snapshot.median_household_income is not None
    ):
        monthly_income = snapshot.median_household_income / Decimal("12")

        if monthly_income > 0:
            payment_to_income = (monthly_payment / monthly_income) * Decimal("100")

            metrics.append(
                DerivedMetric(
                    metric_name="payment_to_income_ratio",
                    metric_value=_ratio(payment_to_income),
                    metric_unit="percent",
                    transformation_notes=(
                        "Derived as estimated_monthly_payment / monthly household income * 100."
                    ),
                    quality_flags={
                        "home_price_source": home_price_source,
                        "requires_mortgage_rate": True,
                    },
                )
            )

    if _positive(monthly_rent) and _positive(home_price):
        annual_rent = monthly_rent * Decimal("12")
        rent_to_price = (annual_rent / home_price) * Decimal("100")

        metrics.append(
            DerivedMetric(
                metric_name="rent_to_price_ratio",
                metric_value=_ratio(rent_to_price),
                metric_unit="percent",
                transformation_notes=(
                    f"Derived as annualized {rent_source} / {home_price_source} * 100."
                ),
                quality_flags={
                    "rent_source": rent_source,
                    "home_price_source": home_price_source,
                },
            )
        )

    if _positive(snapshot.home_price_index) and _positive(snapshot.cpi):
        real_hpi = snapshot.home_price_index / snapshot.cpi * Decimal("100")

        metrics.append(
            DerivedMetric(
                metric_name="real_home_price_index",
                metric_value=_ratio(real_hpi),
                metric_unit="index",
                transformation_notes="Derived as home_price_index / CPI * 100.",
                quality_flags={
                    "requires_home_price_index": True,
                    "requires_cpi": True,
                },
            )
        )

    permits = snapshot.permit_units or snapshot.building_permits

    if _positive(permits) and _positive(snapshot.population):
        permits_per_1000 = permits / snapshot.population * Decimal("1000")

        metrics.append(
            DerivedMetric(
                metric_name="permits_per_1000_people",
                metric_value=_ratio(permits_per_1000),
                metric_unit="count_per_1000_people",
                transformation_notes="Derived as permit units / population * 1000.",
                quality_flags={
                    "permit_source": "permit_units"
                    if snapshot.permit_units is not None
                    else "building_permits",
                    "requires_population": True,
                },
            )
        )

    return metrics


def build_records(
    snapshots: list[CanonicalMarketSnapshot],
    transform_run_id: str,
) -> list[MarketMetricRecord]:
    records: list[MarketMetricRecord] = []

    for snapshot in snapshots:
        for derived_metric in derive_snapshot_metrics(snapshot):
            records.append(
                MarketMetricRecord(
                    geo_id=snapshot.geo_id,
                    period_month=snapshot.period_month,
                    metric_name=derived_metric.metric_name,
                    metric_value=derived_metric.metric_value,
                    metric_unit=derived_metric.metric_unit,
                    source=SOURCE,
                    dataset=DATASET,
                    source_file_id=None,
                    pipeline_run_id=transform_run_id,
                    source_value=derived_metric.metric_value,
                    source_period=snapshot.period_month,
                    period_grain="monthly",
                    transformation_notes=derived_metric.transformation_notes,
                    source_flags={
                        "derived_from": "analytics.market_monthly_metrics",
                    },
                    quality_flags=derived_metric.quality_flags,
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
            "target_metrics": [
                "estimated_monthly_payment",
                "payment_to_income_ratio",
                "price_to_income_ratio",
                "rent_to_price_ratio",
                "real_home_price_index",
                "permits_per_1000_people",
            ],
            "loan_to_value": str(LOAN_TO_VALUE),
            "mortgage_term_months": int(MORTGAGE_TERM_MONTHS),
        },
    )

    try:
        snapshots = fetch_canonical_snapshots()
        metric_records = build_records(snapshots, transform_run_id)
        loaded_count = upsert_market_metrics(metric_records)

        finish_transform_run(
            run_id=transform_run_id,
            status="success",
            records_extracted=len(snapshots),
            records_loaded=loaded_count,
            records_failed=0,
            error_message=None,
        )

        print(
            f"Derived market ratios transform complete. "
            f"Input snapshots: {len(snapshots)}. "
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
