import time
import uuid
from collections.abc import Callable

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from structlog.contextvars import bind_contextvars, clear_contextvars

from app.core.config import settings
from app.core.errors import register_exception_handlers
from app.core.logging import configure_logging, get_logger
from app.routers.audit import router as audit_router
from app.routers.compare import router as compare_router
from app.routers.geo import router as geo_router
from app.routers.health import router as health_router
from app.routers.map import router as map_router
from app.routers.metrics import router as metrics_router
from app.routers.markets import router as markets_router

configure_logging()
logger = get_logger(__name__)


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description="API for real estate market-cycle intelligence.",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.add_middleware(
        GZipMiddleware,
        minimum_size=1000,
    )

    register_exception_handlers(app)

    @app.middleware("http")
    async def request_context_middleware(
        request: Request,
        call_next: Callable[[Request], Response],
    ) -> Response:
        clear_contextvars()

        request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
        request.state.request_id = request_id

        bind_contextvars(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
        )

        start_time = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception:
            logger.exception("request_failed")
            raise

        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

        response.headers["x-request-id"] = request_id

        logger.info(
            "request_completed",
            status_code=response.status_code,
            duration_ms=duration_ms,
        )

        clear_contextvars()
        return response

    app.include_router(health_router)
    app.include_router(geo_router)
    app.include_router(audit_router)
    app.include_router(compare_router)
    app.include_router(markets_router)
    app.include_router(map_router)
    app.include_router(metrics_router)

    return app


app = create_app()
