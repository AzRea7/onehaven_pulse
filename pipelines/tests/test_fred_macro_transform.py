from datetime import date
from decimal import Decimal

from pipelines.transforms.fred.macro_transform import (
    RawFredObservation,
    _month_start,
    _parse_decimal,
    build_records,
)


def test_month_start():
    assert _month_start(date(2026, 5, 17)) == date(2026, 5, 1)


def test_parse_decimal_rejects_fred_missing_value():
    assert _parse_decimal(".") is None
    assert _parse_decimal("") is None
    assert _parse_decimal(None) is None


def test_parse_decimal_accepts_numeric_value():
    assert _parse_decimal("6.875") == Decimal("6.875")


def test_build_records_monthly_average_for_daily_treasury():
    observations = [
        RawFredObservation(
            series_id="DGS10",
            observation_date=date(2026, 1, 2),
            value=Decimal("4.00"),
            source_file_id="source_file_1",
        ),
        RawFredObservation(
            series_id="DGS10",
            observation_date=date(2026, 1, 3),
            value=Decimal("4.20"),
            source_file_id="source_file_1",
        ),
    ]

    records = build_records(observations, "transform_test")

    assert len(records) == 1
    assert records[0].geo_id == "us"
    assert records[0].period_month == date(2026, 1, 1)
    assert records[0].metric_name == "treasury_10yr_rate"
    assert records[0].metric_value == Decimal("4.1000")
    assert records[0].source_flags["aggregation_method"] == "monthly_average"


def test_build_records_monthly_point_for_unemployment():
    observations = [
        RawFredObservation(
            series_id="UNRATE",
            observation_date=date(2026, 1, 1),
            value=Decimal("4.1"),
            source_file_id="source_file_1",
        )
    ]

    records = build_records(observations, "transform_test")

    assert len(records) == 1
    assert records[0].metric_name == "unemployment_rate"
    assert records[0].metric_value == Decimal("4.1000")
    assert records[0].source_flags["aggregation_method"] == "monthly_point"


def test_build_records_monthly_max_for_recession_indicator():
    observations = [
        RawFredObservation(
            series_id="USREC",
            observation_date=date(2026, 1, 1),
            value=Decimal("0"),
            source_file_id="source_file_1",
        ),
        RawFredObservation(
            series_id="USREC",
            observation_date=date(2026, 1, 15),
            value=Decimal("1"),
            source_file_id="source_file_1",
        ),
    ]

    records = build_records(observations, "transform_test")

    assert len(records) == 1
    assert records[0].metric_name == "recession_indicator"
    assert records[0].metric_value == Decimal("1.0000")
    assert records[0].source_flags["aggregation_method"] == "monthly_max"
