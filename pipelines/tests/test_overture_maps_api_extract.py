from pipelines.extractors.overture_maps_api.extract import _infer_columns


def test_infer_columns_from_records():
    records = [
        {"id": "1", "geometry": {}, "properties": {}},
        {"id": "2", "extra": "value"},
    ]

    columns = _infer_columns(records)

    assert "id" in columns
    assert "geometry" in columns
    assert "properties" in columns
    assert "extra" in columns


def test_infer_columns_empty():
    assert _infer_columns([]) == []
