from pipelines.extractors.census_acs.config import (
    ACS_PROFILE_VARIABLES,
    CENSUS_ACS_COUNTY,
    CENSUS_ACS_DATASETS,
    CENSUS_ACS_METRO,
    CENSUS_ACS_STATE,
)


def test_census_acs_datasets_configured():
    geography_levels = {dataset.geography_level for dataset in CENSUS_ACS_DATASETS}

    assert "state" in geography_levels
    assert "county" in geography_levels
    assert "metro" in geography_levels


def test_census_acs_variables_include_core_market_fundamentals():
    assert "NAME" in ACS_PROFILE_VARIABLES
    assert "DP05_0001E" in ACS_PROFILE_VARIABLES
    assert "DP03_0062E" in ACS_PROFILE_VARIABLES
    assert "DP04_0001E" in ACS_PROFILE_VARIABLES
    assert "DP04_0089E" in ACS_PROFILE_VARIABLES


def test_census_acs_geography_params():
    assert CENSUS_ACS_STATE.params["for"] == "state:*"
    assert CENSUS_ACS_COUNTY.params["for"] == "county:*"
    assert "metropolitan statistical area" in CENSUS_ACS_METRO.params["for"]
