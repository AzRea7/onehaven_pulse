from fastapi import FastAPI
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()

app = FastAPI(
    title="OneHaven Market Engine API",
    version="0.1.0",
    description="API for real estate market-cycle intelligence.",
)

engine = create_engine(settings.database_url, pool_pre_ping=True)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "onehaven-market-api",
    }


@app.get("/health/db")
def database_health():
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT PostGIS_Version();"))
            postgis_version = result.scalar()

        return {
            "status": "ok",
            "database": "connected",
            "postgis_version": postgis_version,
        }

    except SQLAlchemyError as exc:
        return {
            "status": "error",
            "database": "unavailable",
            "detail": str(exc),
        }
