from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.db.session import engine
from app.routers.audit import router as audit_router
from app.routers.geo import router as geo_router
from app.routers.markets import router as markets_router

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

app.include_router(geo_router)
app.include_router(audit_router)
app.include_router(markets_router)


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
