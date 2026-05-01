from datetime import date
from decimal import Decimal

from pipelines.transforms.derived.market_ratios_transform import (
    CanonicalMarketSnapshot,
    _monthly_payment,
    derive_snapshot_metrics,
)


def test_monthly_payment():
    payment = _monthly_payment(
        home_price=Decimal("500000"),
        annual_rate_percent=Decimal("6.0"),
    )

    assert payment is not None
    assert payment > Decimal("0")


def test_derive_affordability_metrics():
    snapshot = CanonicalMarketSnapshot(
        geo_id="state:26",
        period_month=date(2026, 1, 1),
        median_sale_price=Decimal("300000"),
        zhvi=None,
        home_price_index=Decimal("250"),
        cpi=Decimal("300"),
        median_household_income=Decimal("75000"),
        median_rent=Decimal("1500"),
        zori=None,
        mortgage_rate_30y=Decimal("6.5"),
        permit_units=Decimal("100"),
        building_permits=None,
        population=Decimal("1000000"),
    )

    metrics = derive_snapshot_metrics(snapshot)
    metric_names = {metric.metric_name for metric in metrics}

    assert "estimated_monthly_payment" in metric_names
    assert "payment_to_income_ratio" in metric_names
    assert "price_to_income_ratio" in metric_names
    assert "rent_to_price_ratio" in metric_names
    assert "real_home_price_index" in metric_names
    assert "permits_per_1000_people" in metric_names
