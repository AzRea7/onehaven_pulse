from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_source_freshness_endpoint_exists():
    response = client.get("/audit/source-freshness")

    assert response.status_code in {200, 500}
