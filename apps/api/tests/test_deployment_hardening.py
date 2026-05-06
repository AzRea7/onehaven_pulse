import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.core.config import Settings
from app.main import app

client = TestClient(app)


def test_health_is_liveness_only():
    response = client.get("/health")

    assert response.status_code == 200

    payload = response.json()

    assert payload["status"] == "healthy"
    assert payload["database"] == "not_checked"


def test_ready_checks_database():
    response = client.get("/ready")

    assert response.status_code == 200

    payload = response.json()

    assert payload["status"] == "ready"
    assert payload["database"] == "connected"
    assert payload["postgis"]


def test_production_rejects_wildcard_cors():
    with pytest.raises(ValidationError):
        Settings(
            ENVIRONMENT="production",
            DATABASE_URL="postgresql+psycopg2://user:strong_password@db.example.com:5432/onehaven",
            FRONTEND_ORIGIN="https://app.example.com",
            CORS_ALLOW_ORIGINS="*",
        )


def test_production_rejects_localhost_database():
    with pytest.raises(ValidationError):
        Settings(
            ENVIRONMENT="production",
            DATABASE_URL="postgresql+psycopg2://user:strong_password@localhost:5432/onehaven",
            FRONTEND_ORIGIN="https://app.example.com",
            CORS_ALLOW_ORIGINS="https://app.example.com",
        )


def test_production_rejects_dev_password():
    with pytest.raises(ValidationError):
        Settings(
            ENVIRONMENT="production",
            DATABASE_URL=(
                "postgresql+psycopg2://onehaven:onehaven_dev_password"
                "@db.example.com:5432/onehaven"
            ),
            FRONTEND_ORIGIN="https://app.example.com",
            CORS_ALLOW_ORIGINS="https://app.example.com",
        )


def test_local_allows_localhost_cors_and_db():
    settings = Settings(
        ENVIRONMENT="local",
        DATABASE_URL="postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market",
        FRONTEND_ORIGIN="http://localhost:3000",
        CORS_ALLOW_ORIGINS="http://localhost:3000,http://127.0.0.1:3000",
    )

    assert "http://localhost:3000" in settings.allowed_origins
    assert "@localhost:" in settings.database_url
