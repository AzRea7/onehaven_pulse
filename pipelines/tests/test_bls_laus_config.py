from pipelines.extractors.bls_laus.config import BLS_LAUS_DATASET, BLS_LAUS_SERIES


def test_bls_laus_dataset_configured():
    assert BLS_LAUS_DATASET.dataset == "labor_market"
    assert BLS_LAUS_DATASET.expected_frequency == "monthly"
    assert BLS_LAUS_DATASET.filename.endswith(".json")


def test_bls_laus_series_configured():
    series_ids = {series.series_id for series in BLS_LAUS_SERIES}

    assert "LNS14000000" in series_ids
    assert "LASST260000000000003" in series_ids
    assert "LASST480000000000003" in series_ids
    assert "LASST120000000000003" in series_ids
