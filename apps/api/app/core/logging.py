import logging
import os
import re
import sys
from collections.abc import Mapping
from typing import Any

import structlog

from app.core.config import settings


SECRET_KEY_PATTERN = re.compile(
    r"(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|refresh[_-]?token|authorization|cookie)",
    re.IGNORECASE,
)

REDACTED = "[REDACTED]"


def _redact_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return sanitize_log_payload(dict(value))

    if isinstance(value, list):
        return [_redact_value(item) for item in value]

    if isinstance(value, tuple):
        return tuple(_redact_value(item) for item in value)

    return value


def sanitize_log_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}

    for key, value in payload.items():
        if SECRET_KEY_PATTERN.search(str(key)):
            sanitized[key] = REDACTED
        else:
            sanitized[key] = _redact_value(value)

    return sanitized


def redact_secrets_processor(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    return sanitize_log_payload(event_dict)


def add_service_metadata(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    event_dict.setdefault("service", "onehaven-api")
    event_dict.setdefault("environment", settings.environment)
    event_dict.setdefault("app_version", settings.app_version)
    return event_dict


def configure_logging() -> None:
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)

    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        add_service_metadata,
        redact_secrets_processor,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        timestamper,
        structlog.processors.JSONRenderer(),
    ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    # Reduce noisy third-party access logs. The app emits its own structured request logs.
    logging.getLogger("uvicorn.access").disabled = os.getenv(
        "DISABLE_UVICORN_ACCESS_LOG",
        "true",
    ).lower() in {"1", "true", "yes"}


def get_logger(name: str, **bound_values: Any) -> structlog.BoundLogger:
    logger = structlog.get_logger(name)

    if bound_values:
        logger = logger.bind(**sanitize_log_payload(bound_values))

    return logger
