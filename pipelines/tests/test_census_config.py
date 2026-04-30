from pipelines.extractors.census.config import CENSUS_CBSA, CENSUS_GEOGRAPHY_DATASETS, CENSUS_STATE


def test_census_geography_datasets_configured():
    datasets = {dataset.dataset for dataset in CENSUS_GEOGRAPHY_DATASETS}

    assert "state_boundaries" in datasets
    assert "cbsa_boundaries" in datasets


def test_census_state_config():
    assert CENSUS_STATE.geo_type == "state"
    assert CENSUS_STATE.filename.endswith(".zip")
    assert CENSUS_STATE.url.startswith("https://www2.census.gov/")


def test_census_cbsa_config():
    assert CENSUS_CBSA.geo_type == "metro"
    assert CENSUS_CBSA.filename.endswith(".zip")
    assert CENSUS_CBSA.url.startswith("https://www2.census.gov/")
