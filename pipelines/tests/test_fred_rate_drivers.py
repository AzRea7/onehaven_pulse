from pipelines.extractors.fred.config import FRED_SERIES_BY_ID


def test_rate_driver_metadata_supports_future_spread_calculations():
    mortgage = FRED_SERIES_BY_ID["MORTGAGE30US"]
    ten_year = FRED_SERIES_BY_ID["DGS10"]
    two_year = FRED_SERIES_BY_ID["DGS2"]
    ten_two_spread = FRED_SERIES_BY_ID["T10Y2Y"]
    ten_three_month_spread = FRED_SERIES_BY_ID["T10Y3M"]

    assert mortgage.source_role == "mortgage_rate_outcome"
    assert ten_year.source_role == "mortgage_rate_driver"
    assert two_year.category == "treasury_curve"
    assert ten_two_spread.source_role == "yield_curve_inversion_signal"
    assert ten_three_month_spread.source_role == "recession_warning_signal"
