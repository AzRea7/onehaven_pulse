from datetime import UTC, date, datetime
from decimal import Decimal

from pipelines.transforms.common.market_metric_loader import (
    _mart_metric_value,
    _metric_column,
    _record_to_params,
)
from pipelines.transforms.common.market_metric_record import MarketMetricRecord


def make_record(
    *,
    geo_id: str = "metro_19820",
    period_month: date = date(2026, 3, 1),
    metric_name: str = "zhvi",
    metric_value=Decimal("250000.00"),
    source: str = "zillow",
    dataset: str = "zhvi",
) -> MarketMetricRecord:
    return MarketMetricRecord(
        geo_id=geo_id,
        period_month=period_month,
        metric_name=metric_name,
        metric_value=metric_value,
        metric_unit="index",
        source=source,
        dataset=dataset,
        source_file_id="test_source_file",
        pipeline_run_id="test_pipeline_run",
        source_value=metric_value,
        source_period=period_month,
        transformation_notes="test",
        source_flags={"test": True},
        quality_flags={"quality": "test"},
    )


def test_metric_column_known_metric():
    assert _metric_column("zhvi") == "zhvi"
    assert _metric_column("zori_yoy") == "zori_yoy"
    assert _metric_column("unemployment_rate") == "unemployment_rate"


def test_recession_indicator_converts_to_bool():
    record = make_record(
        metric_name="recession_indicator",
        metric_value=Decimal("1"),
        source="fred",
        dataset="macro",
    )

    assert _mart_metric_value(record) is True


def test_record_to_params_contains_required_loader_fields():
    record = make_record()

    params = _record_to_params(record)

    assert params["geo_id"] == "metro_19820"
    assert params["period_month"] == date(2026, 3, 1)
    assert params["metric_name"] == "zhvi"
    assert params["metric_value"] == Decimal("250000.00")
    assert params["source"] == "zillow"
    assert params["dataset"] == "zhvi"
    assert params["source_file_id"] == "test_source_file"
    assert params["pipeline_run_id"] == "test_pipeline_run"
    assert params["source_value"] == Decimal("250000.00")
    assert params["normalized_value"] == Decimal("250000.00")
    assert params["source_period"] == date(2026, 3, 1)
    assert params["transformation_notes"] == "test"
    assert "source_flags" in params
    assert "quality_flags" in params
    assert isinstance(params["created_at"], datetime)
    assert params["created_at"].tzinfo is not None
    assert isinstance(params["updated_at"], datetime)
    assert params["updated_at"].tzinfo is not None
