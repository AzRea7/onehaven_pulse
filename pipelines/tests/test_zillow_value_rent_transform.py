from datetime import date
from decimal import Decimal

from pipelines.transforms.zillow.value_rent_transform import (
    RawZillowRecord,
    _parse_decimal,
    _quantize_pct,
    _quantize_value,
    _subtract_months,
    build_records,
)


def test_parse_decimal():
    assert _parse_decimal("123.45") == Decimal("123.45")
    assert _parse_decimal("") is None
    assert _parse_decimal(".") is None
    assert _parse_decimal(None) is None


def test_quantize_value():
    assert _quantize_value(Decimal("123.456")) == Decimal("123.46")


def test_quantize_pct():
    assert _quantize_pct(Decimal("1.2345678")) == Decimal("1.234568")


def test_subtract_months():
    assert _subtract_months(date(2026, 1, 1), 1) == date(2025, 12, 1)
    assert _subtract_months(date(2026, 1, 1), 12) == date(2025, 1, 1)


def test_build_records_for_country_zhvi_growth():
    records = [
        RawZillowRecord(
            dataset="zhvi",
            source_region_id="102001",
            region_name="United States",
            region_type="country",
            state_name=None,
            metro=None,
            county_name=None,
            period_month=date(2025, 1, 1),
            value=Decimal("100"),
            source_file_id="source_file_1",
        ),
        RawZillowRecord(
            dataset="zhvi",
            source_region_id="102001",
            region_name="United States",
            region_type="country",
            state_name=None,
            metro=None,
            county_name=None,
            period_month=date(2025, 2, 1),
            value=Decimal("110"),
            source_file_id="source_file_1",
        ),
        RawZillowRecord(
            dataset="zhvi",
            source_region_id="102001",
            region_name="United States",
            region_type="country",
            state_name=None,
            metro=None,
            county_name=None,
            period_month=date(2026, 1, 1),
            value=Decimal("120"),
            source_file_id="source_file_1",
        ),
    ]

    metric_records, unmatched = build_records(records, "transform_test")
    metric_names = [record.metric_name for record in metric_records]

    assert unmatched == []
    assert "zhvi" in metric_names
    assert "zhvi_mom" in metric_names
    assert "zhvi_yoy" in metric_names
