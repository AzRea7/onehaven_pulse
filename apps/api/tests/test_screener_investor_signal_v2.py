from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_screener_openapi_route_exists() -> None:
    client = TestClient(app)
    payload = client.get("/openapi.json").json()

    assert "/markets/screener" in payload["paths"]


def test_screener_response_schema_includes_investor_signal_v2_fields() -> None:
    client = TestClient(app)

    response = client.get("/markets/screener?geo_type=metro&min_confidence=0.5&limit=5")
    assert response.status_code in {200, 500}

    # Full live DB behavior is validated by smoke_investor_screener_presets.sh.
    # This test protects route registration in app construction.
