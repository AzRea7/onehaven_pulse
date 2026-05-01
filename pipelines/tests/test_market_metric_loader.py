from datetime import date
from decimal import Decimal

from pipelines.transforms.common.market_metric_loader import _metric_column, _record_to_params
from pipelines.transforms.common.market_metric_record import MarketMetricRecord


def test_metric_column_maps_supported_metric():
    assert _metric_column("home_price_index") == "home_price_index"


def test_metric_column_rejects_unsupported_metric():
    try:
        _metric_column("fake_metric")
    except ValueError as exc:
        assert "Unsupported metric_name" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_record_to_params():
    record = MarketMetricRecord(
        geo_id="us",
        period_month=date(2026, 1, 1),
        metric_name="home_price_index",
        metric_value=Decimal("100.0"),
        metric_unit="index",
        source="test",
        dataset="test_dataset",
        source_flags={"raw": True},
        quality_flags={"valid": True},
    )

    params = _record_to_params(record)

    assert params["geo_id"] == "us"
    assert params["period_month"] == date(2026, 1, 1)
    assert params["metric_name"] == "home_price_index"
    assert params["metric_value"] == Decimal("100.0")
    assert params["source"] == "test"
    assert params["dataset"] == "test_dataset"
    assert params["created_at"]
    assert params["updated_at"]


def test_metric_column_maps_fred_macro_metrics():
    assert _metric_column("treasury_10yr_rate") == "treasury_10yr_rate"
    assert _metric_column("treasury_10yr_2yr_spread") == "treasury_10yr_2yr_spread"
    assert _metric_column("recession_indicator") == "recession_indicator"


def test_recession_indicator_converts_to_boolean_for_mart_value():
    from datetime import date
    from decimal import Decimal

    from pipelines.transforms.common.market_metric_loader import _record_to_params
    from pipelines.transforms.common.market_metric_record import MarketMetricRecord

    record = MarketMetricRecord(
        geo_id="us",
        period_month=date(2026, 1, 1),
        metric_name="recession_indicator",
        metric_value=Decimal("1.0000"),
        metric_unit="binary",
        source="fred",
        dataset="macro_series",
    )

    params = _record_to_params(record)

    assert params["metric_value"] is True
    assert params["normalized_value"] == Decimal("1.0000")
