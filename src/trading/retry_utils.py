import asyncio
import os
import random
import re
import time
from typing import Awaitable, Callable, Optional, TypeVar

from trading.logging_utils import get_logger, service_message


logger = get_logger(__name__)

T = TypeVar("T")

DEFAULT_RETRY_ATTEMPTS = int(os.getenv("EXTERNAL_API_RETRY_ATTEMPTS", "3"))
DEFAULT_RETRY_BASE_DELAY_SECONDS = float(os.getenv("EXTERNAL_API_RETRY_BASE_DELAY_SECONDS", "0.5"))
DEFAULT_RETRY_MAX_DELAY_SECONDS = float(os.getenv("EXTERNAL_API_RETRY_MAX_DELAY_SECONDS", "4.0"))

TRANSIENT_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}
TRANSIENT_ERROR_TEXT = (
    "connection aborted",
    "connection reset",
    "cannot connect",
    "internal server error",
    "network error",
    "rate limit",
    "temporarily unavailable",
    "timed out",
    "timeout",
    "too many requests",
)


def _status_code(exc: Exception) -> Optional[int]:
    for attr in ("status", "status_code", "code"):
        raw = getattr(exc, attr, None)
        try:
            status = int(raw)
        except (TypeError, ValueError):
            continue
        if 100 <= status <= 599:
            return status
    return None


def is_transient_error(exc: Exception) -> bool:
    status = _status_code(exc)
    if status in TRANSIENT_STATUS_CODES or (status is not None and status >= 500):
        return True

    message = str(exc).lower()
    status_match = re.search(r"\b(?:http\s+error|status)\s+(\d{3})\b", message)
    if status_match:
        parsed_status = int(status_match.group(1))
        if parsed_status in TRANSIENT_STATUS_CODES or parsed_status >= 500:
            return True

    return any(token in message for token in TRANSIENT_ERROR_TEXT)


def _retry_delay(attempt_index: int, *, base_delay: float, max_delay: float) -> float:
    exponential_delay = min(max_delay, base_delay * (2 ** attempt_index))
    jitter = random.uniform(0, exponential_delay * 0.25)
    return exponential_delay + jitter


def call_with_retries(
    operation: Callable[[], T],
    *,
    service: str,
    action: str,
    attempts: int = DEFAULT_RETRY_ATTEMPTS,
    base_delay: float = DEFAULT_RETRY_BASE_DELAY_SECONDS,
    max_delay: float = DEFAULT_RETRY_MAX_DELAY_SECONDS,
) -> T:
    attempts = max(1, attempts)
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            if attempt >= attempts or not is_transient_error(exc):
                raise

            delay = _retry_delay(attempt - 1, base_delay=base_delay, max_delay=max_delay)
            logger.warning(
                service_message(
                    service,
                    "Transient error during %s attempt %s/%s; retrying in %.2fs: %s",
                ),
                action,
                attempt,
                attempts,
                delay,
                exc,
            )
            time.sleep(delay)

    raise RuntimeError(f"{service} {action} retry loop exited unexpectedly.")


async def async_call_with_retries(
    operation: Callable[[], Awaitable[T]],
    *,
    service: str,
    action: str,
    attempts: int = DEFAULT_RETRY_ATTEMPTS,
    base_delay: float = DEFAULT_RETRY_BASE_DELAY_SECONDS,
    max_delay: float = DEFAULT_RETRY_MAX_DELAY_SECONDS,
) -> T:
    attempts = max(1, attempts)
    for attempt in range(1, attempts + 1):
        try:
            return await operation()
        except Exception as exc:
            if attempt >= attempts or not is_transient_error(exc):
                raise

            delay = _retry_delay(attempt - 1, base_delay=base_delay, max_delay=max_delay)
            logger.warning(
                service_message(
                    service,
                    "Transient error during %s attempt %s/%s; retrying in %.2fs: %s",
                ),
                action,
                attempt,
                attempts,
                delay,
                exc,
            )
            await asyncio.sleep(delay)

    raise RuntimeError(f"{service} {action} retry loop exited unexpectedly.")
