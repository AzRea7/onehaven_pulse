from datetime import date
from decimal import Decimal

from pipelines.transforms.census_building_permits.permits_transform import (
    RawCensusBpsRecord,
    _quantize_count,
    build_records,
)


def test_quantize_count():
    assert _quantize_count(Decimal("123.456")) == Decimal("123.46")


def test_build_records_national():
    raw_records = [
        RawCensusBpsRecord(
            geography_level="national",
            period_type="monthly",
            source_period_label="2026-01",
            source_geo_id="us",
            source_name="United States",
            state_fips=None,
            county_fips=None,
            cbsa_code=None,
            period_month=date(2026, 1, 1),
            building_permits=Decimal("1000"),
            single_family_permits=Decimal("700"),
            multi_family_permits=Decimal("300"),
            permit_units=Decimal("1200"),
            source_file_id="source_file_1",
        )
    ]

    metric_records, unmatched = build_records(raw_records, "transform_test")
    metric_names = {record.metric_name for record in metric_records}

    assert unmatched == []
    assert "building_permits" in metric_names
    assert "single_family_permits" in metric_names
    assert "multi_family_permits" in metric_names
    assert "permit_units" in metric_names
