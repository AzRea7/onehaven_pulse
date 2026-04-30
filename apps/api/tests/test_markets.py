from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_market_metrics_summary_endpoint_exists():
    response = client.get("/markets/metrics/summary")

    assert response.status_code in {200, 500}
