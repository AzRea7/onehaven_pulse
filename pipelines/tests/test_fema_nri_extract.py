from pipelines.extractors.fema_nri.extract import _infer_columns


def test_infer_columns_from_records():
    records = [
        {"COUNTYFIPS": "01001", "RISK_SCORE": 50.1},
        {"COUNTY": "Autauga", "STATE": "Alabama"},
    ]

    columns = _infer_columns(records)

    assert "COUNTYFIPS" in columns
    assert "RISK_SCORE" in columns
    assert "COUNTY" in columns
    assert "STATE" in columns


def test_infer_columns_empty_records():
    assert _infer_columns([]) == []
