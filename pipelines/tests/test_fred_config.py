from pipelines.extractors.fred.config import FRED_SERIES


def test_fred_series_configured():
    assert len(FRED_SERIES) > 0

    series_ids = {series.series_id for series in FRED_SERIES}

    assert "MORTGAGE30US" in series_ids
    assert "CPIAUCSL" in series_ids
    assert "UNRATE" in series_ids
    assert "FEDFUNDS" in series_ids
    assert "USREC" in series_ids


def test_fred_series_contains_rate_driver_series():
    series_ids = {series.series_id for series in FRED_SERIES}

    assert "DGS2" in series_ids
    assert "DGS5" in series_ids
    assert "DGS10" in series_ids
    assert "DGS30" in series_ids
    assert "T10Y2Y" in series_ids
    assert "T10Y3M" in series_ids


def test_fred_series_has_unique_ids():
    series_ids = [series.series_id for series in FRED_SERIES]

    assert len(series_ids) == len(set(series_ids))


def test_fred_series_definitions_are_complete():
    for series in FRED_SERIES:
        assert series.series_id
        assert series.metric_name
        assert series.frequency_hint
        assert series.category
        assert series.description
