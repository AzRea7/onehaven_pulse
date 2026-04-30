from pipelines.extractors.zillow.config import ZILLOW_DATASETS, ZILLOW_ZHVI, ZILLOW_ZORI


def test_zillow_datasets_configured():
    dataset_names = {dataset.dataset for dataset in ZILLOW_DATASETS}

    assert "zhvi" in dataset_names
    assert "zori" in dataset_names


def test_zillow_dataset_metadata():
    assert ZILLOW_ZHVI.metric_name == "zhvi"
    assert ZILLOW_ZORI.metric_name == "zori"
    assert ZILLOW_ZHVI.filename == "zhvi.csv"
    assert ZILLOW_ZORI.filename == "zori.csv"
    assert ZILLOW_ZHVI.expected_frequency == "monthly"
    assert ZILLOW_ZORI.expected_frequency == "monthly"
