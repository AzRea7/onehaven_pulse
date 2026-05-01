from datetime import date
from decimal import Decimal

from pipelines.transforms.bls_laus.labor_market_transform import (
    RawBlsLausObservation,
    _quantize_metric,
    build_records,
)


def test_quantize_metric():
    assert _quantize_metric(Decimal("4.123456"), "unemployment_rate") == Decimal("4.1235")
    assert _quantize_metric(Decimal("123456.789"), "labor_force") == Decimal("123456.79")


def test_build_records_national_labor_metrics():
    raw_records = [
        RawBlsLausObservation(
            series_id="LNS14000000",
            geography_level="national",
            measure="unemployment_rate",
            geo_reference="US",
            period_month=date(2026, 1, 1),
            value=Decimal("4.1"),
            source_file_id="source_file_1",
        ),
        RawBlsLausObservation(
            series_id="LNS11000000",
            geography_level="national",
            measure="labor_force",
            geo_reference="US",
            period_month=date(2026, 1, 1),
            value=Decimal("168000000"),
            source_file_id="source_file_1",
        ),
    ]

    metric_records, unmatched = build_records(raw_records, "transform_test")
    metric_names = {record.metric_name for record in metric_records}

    assert unmatched == []
    assert "unemployment_rate" in metric_names
    assert "labor_force" in metric_names
