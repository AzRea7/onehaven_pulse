import json

from fastapi.testclient import TestClient

from app.core.logging import REDACTED, sanitize_log_payload
from app.main import app

client = TestClient(app)


def test_request_id_and_latency_headers_are_returned():
    response = client.get("/health", headers={"x-request-id": "test-request-123"})

    assert response.status_code == 200
    assert response.headers["x-request-id"] == "test-request-123"
    assert "x-response-time-ms" in response.headers
    assert float(response.headers["x-response-time-ms"]) >= 0


def test_sanitize_log_payload_redacts_secret_context():
    payload = sanitize_log_payload(
        {
            "database_url": "postgresql://user:password@host/db",
            "FRED_API_KEY": "super-secret",
            "nested": {
                "access_token": "token-value",
                "safe": "visible",
            },
            "safe": "visible",
        }
    )

    assert payload["database_url"] == "postgresql://user:password@host/db"
    assert payload["FRED_API_KEY"] == REDACTED
    assert payload["nested"]["access_token"] == REDACTED
    assert payload["nested"]["safe"] == "visible"
    assert payload["safe"] == "visible"


def test_request_logging_outputs_json(capsys):
    response = client.get("/health", headers={"x-request-id": "json-log-test"})

    assert response.status_code == 200

    captured = capsys.readouterr()
    log_lines = [
        line
        for line in captured.out.splitlines()
        if line.strip().startswith("{") and "request_completed" in line
    ]

    assert log_lines, captured.out

    payload = json.loads(log_lines[-1])

    assert payload["event"] == "request_completed"
    assert payload["request_id"] == "json-log-test"
    assert payload["path"] == "/health"
    assert "duration_ms" in payload
    assert payload["status_code"] == 200
