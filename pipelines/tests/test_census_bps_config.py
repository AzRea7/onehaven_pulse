from pipelines.extractors.census_building_permits.config import (
    CENSUS_BPS_CBSA_ANNUAL,
    CENSUS_BPS_CBSA_MONTHLY,
    CENSUS_BPS_DATASETS,
    CENSUS_BPS_STATE_ANNUAL,
    CENSUS_BPS_STATE_MONTHLY,
)


def test_census_bps_datasets_configured():
    keys = {
        (dataset.geography_level, dataset.period_type)
        for dataset in CENSUS_BPS_DATASETS
    }

    assert ("state", "monthly") in keys
    assert ("cbsa", "monthly") in keys
    assert ("state", "annual") in keys
    assert ("cbsa", "annual") in keys


def test_monthly_files_are_xls():
    assert CENSUS_BPS_STATE_MONTHLY.filename.endswith(".xls")
    assert CENSUS_BPS_CBSA_MONTHLY.filename.endswith(".xls")
    assert CENSUS_BPS_STATE_MONTHLY.source_period_label == "2026-01"
    assert CENSUS_BPS_CBSA_MONTHLY.source_period_label == "2026-01"


def test_annual_files_are_xls():
    assert CENSUS_BPS_STATE_ANNUAL.filename.endswith(".xls")
    assert CENSUS_BPS_CBSA_ANNUAL.filename.endswith(".xls")
    assert CENSUS_BPS_STATE_ANNUAL.source_period_label == "2025"
    assert CENSUS_BPS_CBSA_ANNUAL.source_period_label == "2025"
