from datetime import date
from decimal import Decimal

from pipelines.loaders.census_acs_loader import load_census_acs_profile, _record_to_params


def test_census_acs_record_to_params_state():
    headers = [
        "NAME",
        "DP05_0001E",
        "DP03_0062E",
        "DP04_0001E",
        "DP04_0002E",
        "DP04_0003E",
        "DP04_0046E",
        "DP04_0047E",
        "DP04_0089E",
        "DP04_0142PE",
        "state",
    ]
    row = [
        "Michigan",
        "1000",
        "65000",
        "500",
        "450",
        "50",
        "300",
        "150",
        "1200",
        "42.5",
        "26",
    ]

    params = _record_to_params(
        headers=headers,
        row=row,
        geography_level="state",
        year=2024,
        source_period_start=date(2020, 1, 1),
        source_period_end=date(2024, 12, 31),
        source_file_id="source_file_1",
        load_date=date(2026, 5, 1),
    )

    assert params is not None
    assert params["source_geo_id"] == "state:26"
    assert params["source_name"] == "Michigan"
    assert params["total_population"] == Decimal("1000")
    assert params["median_household_income"] == Decimal("65000")
    assert params["rent_burden_pct"] == Decimal("42.5")


def test_load_census_acs_profile_empty_payload_returns_zero():
    assert (
        load_census_acs_profile(
            payload=[],
            geography_level="state",
            year=2024,
            source_period_start=date(2020, 1, 1),
            source_period_end=date(2024, 12, 31),
            source_file_id=None,
            load_date=date(2026, 5, 1),
        )
        == 0
    )
