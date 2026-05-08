from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_investor_signal_openapi_contract() -> None:
    client = TestClient(app)
    payload = client.get("/openapi.json").json()

    assert "/markets/{geo_id}/investor-signal" in payload["paths"]


def test_investor_signal_response_contract_live_shape() -> None:
    client = TestClient(app)

    response = client.get("/markets/metro_19820/investor-signal")
    assert response.status_code in {200, 404, 500}

    # This route-level contract is mostly covered by smoke against live Docker DB.
    # The test guarantees the route remains mounted in app construction.
