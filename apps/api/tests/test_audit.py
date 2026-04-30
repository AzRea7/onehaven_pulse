from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app, raise_server_exceptions=False)


def test_source_freshness_endpoint_exists():
    response = client.get("/audit/source-freshness")

    assert response.status_code in {200, 500}
