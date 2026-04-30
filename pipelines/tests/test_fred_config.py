from pipelines.extractors.fred.config import FRED_SERIES, FRED_SERIES_BY_ID


def test_fred_series_contains_existing_core_series():
    series_ids = {series.series_id for series in FRED_SERIES}

    assert "MORTGAGE30US" in series_ids
    assert "CPIAUCSL" in series_ids
    assert "UNRATE" in series_ids
    assert "FEDFUNDS" in series_ids
    assert "USREC" in series_ids


def test_fred_series_contains_rate_driver_series:
    series_ids = {series.series_id for series in FRED_SERIES}

    assert "DGS2" in series_ids
    assert "DGS5" in series_ids
    assert "DGS10" in series_ids
    assert "DGS30" in series_ids
    assert "T10Y2Y" in series_ids
    assert "T10Y3M" in series_ids


def test_dgs10_is_marked_as_mortgage_rate_driver:
    series = FRED_SERIES_BY_ID["DGS10"]

    assert series.category == "treasury_curve"
    assert series.source_role == "mortgage_rate_driver"


def test_fred_series_ids_are_unique:
    series_ids = [series.series_id for series in FRED_SERIES]

    assert len(series_ids) == len(set(series_ids))
