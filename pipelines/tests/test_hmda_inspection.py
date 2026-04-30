from pipelines.extractors.hmda.extract import _inspect_csv


def test_inspect_hmda_csv_with_header():
    content = (
        b"activity_year,lei,state_code,county_code,census_tract,loan_amount,action_taken\n"
        b"2025,TESTLEI,MI,163,26163500000,250000,1\n"
    )

    inspection = _inspect_csv(content)

    assert inspection["row_count"] == 1
    assert inspection["likely_has_header"] is True
    assert "loan_amount" in inspection["columns"]


def test_inspect_hmda_csv_without_header():
    content = b"2025,TESTLEI,MI,163,26163500000,250000,1\n"

    inspection = _inspect_csv(content)

    assert inspection["row_count"] == 1
    assert inspection["likely_has_header"] is False
    assert inspection["columns"] == []
