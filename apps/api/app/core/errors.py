from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError
from starlette import status

from app.core.logging import get_logger, sanitize_log_payload

logger = get_logger(__name__)


class ApiError(Exception):
    def __init__(
        self,
        *,
        status_code: int,
        code: str,
        message: str,
        details: dict[str, Any] | list[Any] | None = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details


def error_response(
    *,
    status_code: int,
    code: str,
    message: str,
    request: Request,
    details: object | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "details": sanitize_log_payload({"details": details})["details"]
                if isinstance(details, dict)
                else details,
                "request_id": getattr(request.state, "request_id", None),
            }
        },
    )


def _request_context(request: Request) -> dict[str, Any]:
    return sanitize_log_payload(
        {
            "request_id": getattr(request.state, "request_id", None),
            "method": request.method,
            "path": request.url.path,
            "query_params": dict(request.query_params),
            "client_host": request.client.host if request.client else None,
        }
    )


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
        logger.warning(
            "api_error",
            **_request_context(request),
            status_code=exc.status_code,
            error_code=exc.code,
            error_message=exc.message,
            details=exc.details,
        )

        return error_response(
            status_code=exc.status_code,
            code=exc.code,
            message=exc.message,
            request=request,
            details=exc.details,
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        code = "http_error"
        message = str(exc.detail)
        details = None

        if isinstance(exc.detail, dict):
            code = str(exc.detail.get("code", code))
            message = str(exc.detail.get("message", message))
            details = exc.detail.get("details")

        logger.warning(
            "http_error",
            **_request_context(request),
            status_code=exc.status_code,
            error_code=code,
            error_message=message,
            details=details,
        )

        return error_response(
            status_code=exc.status_code,
            code=code,
            message=message,
            request=request,
            details=details,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        logger.warning(
            "request_validation_error",
            **_request_context(request),
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            error_code="validation_error",
            details=exc.errors(),
        )

        return error_response(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="validation_error",
            message="Request validation failed.",
            request=request,
            details=exc.errors(),
        )

    @app.exception_handler(SQLAlchemyError)
    async def sqlalchemy_exception_handler(request: Request, exc: SQLAlchemyError) -> JSONResponse:
        logger.exception(
            "database_error",
            **_request_context(request),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_type=exc.__class__.__name__,
            error_message=str(exc),
        )

        return error_response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code="database_unavailable",
            message="Database is temporarily unavailable.",
            request=request,
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception(
            "unhandled_error",
            **_request_context(request),
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_type=exc.__class__.__name__,
            error_message=str(exc),
        )

        return error_response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            code="internal_server_error",
            message="Internal server error.",
            request=request,
        )
