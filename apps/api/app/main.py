import time
import uuid
from collections.abc import Callable

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from structlog.contextvars import bind_contextvars, clear_contextvars

from app.core.config import settings
from app.ai.router import router as ai_router
from app.mcp.router import router as mcp_router
from app.core.errors import register_exception_handlers
from app.core.logging import configure_logging, get_logger, sanitize_log_payload
from app.routers import geographies
from app.routers.admin import router as admin_router
from app.routers.audit import router as audit_router
from app.routers.compare import router as compare_router
from app.routers.geo import router as geo_router
from app.routers.health import router as health_router
from app.routers.map import router as map_router
from app.routers.markets import router as markets_router
from app.routers.metrics import router as metrics_router

try:
    from app.routers.screener import router as screener_router
except ImportError:  # pragma: no cover - allows older branches without screener router.
    screener_router = None


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
        allow_credentials=settings.cors_allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.add_middleware(
        GZipMiddleware,
        minimum_size=settings.gzip_minimum_size,
    )

    register_exception_handlers(app)

    @app.middleware("http")
    async def request_context_middleware(
        request: Request,
        call_next: Callable[[Request], Response],
    ) -> Response:
        clear_contextvars()

        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = request_id

        request_context = sanitize_log_payload(
            {
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "query_params": dict(request.query_params),
                "client_host": request.client.host if request.client else None,
                "user_agent": request.headers.get("user-agent"),
            }
        )

        bind_contextvars(**request_context)

        start_time = time.perf_counter()

        logger.info("request_started", **request_context)

        try:
            response = await call_next(request)
        except Exception:
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
            logger.exception(
                "request_failed",
                **request_context,
                duration_ms=duration_ms,
            )
            clear_contextvars()
            raise

        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

        response.headers["x-request-id"] = request_id
        response.headers["x-response-time-ms"] = str(duration_ms)

        logger.info(
            "request_completed",
            **request_context,
            status_code=response.status_code,
            duration_ms=duration_ms,
        )

        clear_contextvars()
        return response

    app.include_router(health_router)
    app.include_router(geo_router)
    app.include_router(audit_router)
    app.include_router(admin_router)
    app.include_router(compare_router)
    app.include_router(markets_router)
    app.include_router(map_router)
    app.include_router(metrics_router)
    app.include_router(geographies.router)

    if screener_router is not None:
        app.include_router(screener_router)

    app.include_router(mcp_router)
    app.include_router(ai_router)

    return app


app = create_app()
