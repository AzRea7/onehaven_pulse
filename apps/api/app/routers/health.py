from fastapi import APIRouter, Response, status
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.db.session import engine

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    environment: str
    database: str


class ReadinessResponse(BaseModel):
    status: str
    service: str
    version: str
    environment: str
    database: str
    postgis: str | None = None


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        status="healthy",
        service="onehaven-market-api",
        version=settings.app_version,
        environment=settings.environment,
        database="not_checked",
    )


@router.get("/ready", response_model=ReadinessResponse)
def ready(response: Response) -> ReadinessResponse:
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            postgis_version = connection.execute(text("SELECT PostGIS_Version()")).scalar_one()
    except SQLAlchemyError:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return ReadinessResponse(
            status="not_ready",
            service="onehaven-market-api",
            version=settings.app_version,
            environment=settings.environment,
            database="unavailable",
            postgis=None,
        )

    return ReadinessResponse(
        status="ready",
        service="onehaven-market-api",
        version=settings.app_version,
        environment=settings.environment,
        database="connected",
        postgis=str(postgis_version),
    )
