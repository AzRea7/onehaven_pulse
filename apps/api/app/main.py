from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic_settings import BaseSettings
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


class Settings(BaseSettings):
    database_url: str = (
        "postgresql+psycopg2://onehaven:onehaven_dev_password"
        "@localhost:5432/onehaven_market"
    )
    frontend_origin: str = "http://localhost:3000"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()

app = FastAPI(
    title="OneHaven Market Engine API",
    version="0.1.0",
    description="API for real estate market-cycle intelligence.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        settings.frontend_origin,
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
