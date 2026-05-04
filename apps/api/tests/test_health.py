from fastapi.testclient import TestClient

from app.main import app


def test_health_returns_healthy():
    client = TestClient(app)
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()

    assert payload["status"] == "healthy"
    assert payload["service"] == "onehaven-market-api"
    assert payload["database"] == "connected"
    assert "version" in payload
    assert "environment" in payload
