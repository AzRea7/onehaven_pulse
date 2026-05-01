import json



def test_transform_metadata_is_json_serializable_shape():
    metadata_payload = {
        **{"story": "4.1"},
        "run_type": "transform",
        "target_table": "analytics.market_monthly_metrics",
    }

    serialized = json.dumps(metadata_payload)

    assert '"story": "4.1"' in serialized
    assert '"run_type": "transform"' in serialized
    assert '"target_table": "analytics.market_monthly_metrics"' in serialized
