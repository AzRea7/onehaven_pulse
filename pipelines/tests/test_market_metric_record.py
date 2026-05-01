from datetime import date
from decimal import Decimal

from pipelines.transforms.common.market_metric_record import MarketMetricRecord


def test_market_metric_record_accepts_valid_record():
    record = MarketMetricRecord(
        geo_id="us",
        period_month=date(2026, 1, 1),
        metric_name="home_price_index",
        metric_value=Decimal("100.0"),
        metric_unit="index",
        source="test",
        dataset="test_dataset",
    )

    record.validate()


def test_market_metric_record_rejects_non_month_start_period():
    record = MarketMetricRecord(
        geo_id="us",
        period_month=date(2026, 1, 15),
        metric_name="home_price_index",
        metric_value=Decimal("100.0"),
        metric_unit="index",
        source="test",
        dataset="test_dataset",
    )

    try:
        record.validate()
    except ValueError as exc:
        assert "first day of month" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_market_metric_record_rejects_missing_geo_id():
    record = MarketMetricRecord(
        geo_id="",
        period_month=date(2026, 1, 1),
        metric_name="home_price_index",
        metric_value=Decimal("100.0"),
        metric_unit="index",
        source="test",
        dataset="test_dataset",
    )

    try:
        record.validate()
    except ValueError as exc:
        assert "geo_id" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
