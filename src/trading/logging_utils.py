import logging
import os
import sys
from typing import Any, Dict, Optional


REDACTED_LOG_KEYS = {"apikey", "authorization", "token", "secret", "password"}
LOG_FORMAT = "%(levelname)s %(message)s"


def configure_logging() -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    formatter = logging.Formatter(LOG_FORMAT)

    # In Lambda, replace platform/root handlers so the message body stays deterministic.
    if os.getenv("AWS_LAMBDA_FUNCTION_NAME"):
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        root_logger.handlers.clear()
        root_logger.addHandler(handler)
        return

    # Outside Lambda, avoid clobbering test or local handlers.
    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def service_message(service: str, message: str) -> str:
    return f"[{service}] {message}"


def symbol_message(symbol: str, message: str) -> str:
    return f"[Workflow] [{symbol}] {message}"


def service_symbol_message(service: str, symbol: str, message: str) -> str:
    return f"[{service}] [{symbol}] {message}"


def sanitize_log_fields(fields: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        key: ("***" if key.lower() in REDACTED_LOG_KEYS else value)
        for key, value in (fields or {}).items()
    }


def log_external_request(
    logger: logging.Logger,
    service: str,
    action: str,
    *,
    fields: Optional[Dict[str, Any]] = None,
) -> None:
    logger.info(
        service_message(service, "Request started: action=%s fields=%s"),
        action,
        sanitize_log_fields(fields),
    )


def log_external_response(
    logger: logging.Logger,
    service: str,
    action: str,
    *,
    fields: Optional[Dict[str, Any]] = None,
    details: Optional[str] = None,
) -> None:
    safe_fields = sanitize_log_fields(fields)
    if details:
        logger.info(
            service_message(service, "Request completed: action=%s fields=%s details=%s"),
            action,
            safe_fields,
            details,
        )
        return
    logger.info(service_message(service, "Request completed: action=%s fields=%s"), action, safe_fields)
