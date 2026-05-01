from datetime import date
from decimal import Decimal

from pipelines.transforms.fhfa.hpi_transform import (
    RawFhfaHpiRecord,
    _parse_decimal,
    _period_month,
    _quantize_metric,
    _subtract_months,
)


def test_period_month_normalizes_to_first_day():
    assert _period_month(date(2026, 5, 17)) == date(2026, 5, 1)


def test_parse_decimal_rejects_missing_values():
    assert _parse_decimal(None) is None
    assert _parse_decimal("") is None
    assert _parse_decimal(".") is None


def test_parse_decimal_accepts_numeric_values():
    assert _parse_decimal("123.456") == Decimal("123.456")


def test_quantize_metric():
    assert _quantize_metric(Decimal("123.4567899")) == Decimal("123.456790")


def test_subtract_months():
    assert _subtract_months(date(2026, 1, 1), 1) == date(2025, 12, 1)
    assert _subtract_months(date(2026, 1, 1), 12) == date(2025, 1, 1)


def test_raw_fhfa_hpi_record_shape():
    record = RawFhfaHpiRecord(
        source_geo_name="United States",
        source_geo_type="national",
        period=date(2026, 1, 1),
        frequency="monthly",
        hpi=Decimal("100.0"),
        source_file_id="source_file_1",
    )

    assert record.source_geo_name == "United States"
    assert record.source_geo_type == "national"
    assert record.period == date(2026, 1, 1)
    assert record.hpi == Decimal("100.0")
