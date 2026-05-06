from __future__ import annotations

import logging
import os
import re
import sys
from collections.abc import Mapping
from typing import Any

import structlog

SECRET_KEY_PATTERN = re.compile(
    r"(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|refresh[_-]?token|authorization|cookie)",
    re.IGNORECASE,
)

REDACTED = "[REDACTED]"


def sanitize_log_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}

    for key, value in payload.items():
        if SECRET_KEY_PATTERN.search(str(key)):
            sanitized[key] = REDACTED
        elif isinstance(value, Mapping):
            sanitized[key] = sanitize_log_payload(dict(value))
        elif isinstance(value, list):
            sanitized[key] = [
                sanitize_log_payload(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            sanitized[key] = value

    return sanitized


def redact_secrets_processor(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    return sanitize_log_payload(event_dict)


def add_pipeline_metadata(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    event_dict.setdefault("service", "onehaven-pipelines")
    event_dict.setdefault("environment", os.getenv("ENVIRONMENT", "local"))
    return event_dict


def configure_pipeline_logging() -> None:
    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            add_pipeline_metadata,
            redact_secrets_processor,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )


def get_pipeline_logger(name: str, **bound_values: Any) -> structlog.BoundLogger:
    configure_pipeline_logging()
    logger = structlog.get_logger(name)

    if bound_values:
        logger = logger.bind(**sanitize_log_payload(bound_values))

    return logger
