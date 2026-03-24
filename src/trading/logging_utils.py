import logging
from typing import Any, Dict, Optional


REDACTED_LOG_KEYS = {"apikey", "authorization", "token", "secret", "password"}


def configure_logging() -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    if not root_logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)


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
        "%s request started: action=%s fields=%s",
        service,
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
            "%s request completed: action=%s fields=%s details=%s",
            service,
            action,
            safe_fields,
            details,
        )
        return
    logger.info("%s request completed: action=%s fields=%s", service, action, safe_fields)
