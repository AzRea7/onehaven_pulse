from datetime import date
from decimal import Decimal

from pipelines.transforms.census_acs.profile_transform import (
    RawCensusAcsProfileRecord,
    _period_month_for_year,
    _share,
)


def test_period_month_for_year():
    assert _period_month_for_year(2024) == date(2024, 12, 1)


def test_share():
    assert _share(Decimal("25"), Decimal("100")) == Decimal("25.00")
    assert _share(None, Decimal("100")) is None
    assert _share(Decimal("25"), Decimal("0")) is None


def test_raw_census_acs_profile_record_shape():
    record = RawCensusAcsProfileRecord(
        geography_level="state",
        source_geo_id="state:26",
        source_name="Michigan",
        state_fips="26",
        county_fips=None,
        cbsa_code=None,
        year=2024,
        source_period_start=date(2020, 1, 1),
        source_period_end=date(2024, 12, 31),
        total_population=Decimal("1000"),
        median_household_income=Decimal("65000"),
        total_housing_units=Decimal("500"),
        occupied_housing_units=Decimal("450"),
        vacant_housing_units=Decimal("50"),
        owner_occupied_housing_units=Decimal("300"),
        renter_occupied_housing_units=Decimal("150"),
        median_gross_rent=Decimal("1200"),
        rent_burden_pct=Decimal("42.5"),
        source_file_id="source_file_1",
    )

    assert record.source_geo_id == "state:26"
    assert record.year == 2024
    assert record.rent_burden_pct == Decimal("42.5")
