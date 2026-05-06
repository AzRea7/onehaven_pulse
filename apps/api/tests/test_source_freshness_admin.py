from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_admin_source_freshness_contract():
    response = client.get("/admin/source-freshness?refresh=false")

    assert response.status_code == 200

    payload = response.json()

    assert set(payload.keys()) == {"summary", "items"}
    assert {"total", "stale", "failed", "pending", "healthy"} <= set(payload["summary"].keys())
    assert isinstance(payload["items"], list)

    if not payload["items"]:
        return

    item = payload["items"][0]

    assert {
        "source",
        "dataset",
        "latest_source_period",
        "last_loaded_at",
        "record_count",
        "status",
        "error_message",
        "stale_reason",
        "is_stale",
        "expected_frequency",
        "freshness_threshold_days",
    } <= set(item.keys())


def test_admin_source_freshness_filters_are_supported():
    response = client.get("/admin/source-freshness?source=zillow&refresh=false")

    assert response.status_code == 200

    payload = response.json()

    for item in payload["items"]:
        assert item["source"] == "zillow"
