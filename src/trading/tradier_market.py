import json
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from trading.logging_utils import get_logger, log_external_request, log_external_response, service_message
from trading.retry_utils import call_with_retries


logger = get_logger(__name__)

TRADIER_BASE_URL = "https://api.tradier.com"


class TradierError(RuntimeError):
    pass


def _tradier_token() -> str:
    token = os.getenv("TRADIER_TOKEN", "")
    if not token:
        raise TradierError("TRADIER_TOKEN environment variable is not set.")
    return token


def _market_timezone() -> ZoneInfo:
    return ZoneInfo(os.getenv("MARKET_TIMEZONE", "America/New_York"))


def _parse_market_time(session_date: date, hhmm: str) -> datetime:
    hour_str, minute_str = (hhmm or "").split(":", 1)
    return datetime.combine(
        session_date,
        datetime.min.time().replace(hour=int(hour_str), minute=int(minute_str)),
        tzinfo=_market_timezone(),
    )


def _get_json(path: str, *, params: Optional[Dict[str, str]] = None, timeout: float = 20.0) -> Any:
    url = f"{TRADIER_BASE_URL}{path}"
    request_url = url
    if params:
        request_url = f"{url}?{urlencode(params)}"

    request = Request(
        request_url,
        headers={
            "Authorization": f"Bearer {_tradier_token()}",
            "Accept": "application/json",
        },
        method="GET",
    )

    def _request() -> Any:
        log_external_request(logger, "Tradier", "GET", fields={"url": url, **(params or {})})
        try:
            with urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
                log_external_response(logger, "Tradier", "GET", fields={"url": url, "status": response.status, **(params or {})})
                return payload
        except HTTPError as exc:
            log_external_response(logger, "Tradier", "GET", fields={"url": url, "status": exc.code, **(params or {})}, details="http_error")
            raise TradierError(f"HTTP error {exc.code}: {exc.reason}") from exc
        except URLError as exc:
            logger.warning(service_message("Tradier", "Network request failed: url=%s error=%s"), url, exc.reason)
            raise TradierError(f"Network error: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            logger.warning(service_message("Tradier", "Response was not valid JSON: url=%s"), url)
            raise TradierError("Tradier response was not valid JSON.") from exc

    return call_with_retries(
        _request,
        service="Tradier",
        action="GET",
    )


def get_tradier_market_clock() -> Dict[str, Any]:
    logger.info(service_message("Tradier", "Requesting market clock."))
    data = _get_json("/v1/markets/clock")
    clock = data.get("clock") or {}
    if not clock:
        raise TradierError("Tradier market clock response did not include clock data.")
    logger.info(
        service_message("Tradier", "Fetched market clock: state=%s next_state=%s"),
        clock.get("state"),
        clock.get("next_state"),
    )
    return clock


def _get_market_calendar_days(*, year: int, month: int) -> List[Dict[str, Any]]:
    logger.info(service_message("Tradier", "Requesting market calendar for year=%s month=%s."), year, month)
    data = _get_json("/v1/markets/calendar", params={"year": str(year), "month": f"{month:02d}"})
    days = (data.get("calendar") or {}).get("days", {}).get("day", [])
    if isinstance(days, dict):
        days = [days]
    logger.info(service_message("Tradier", "Fetched %s market calendar day rows for year=%s month=%s."), len(days), year, month)
    return days


def _get_calendar_day(target_date: date) -> Optional[Dict[str, Any]]:
    target_iso = target_date.isoformat()
    for day in _get_market_calendar_days(year=target_date.year, month=target_date.month):
        if day.get("date") == target_iso:
            return day
    return None


def get_tradier_session_window(target_date: date) -> Optional[Tuple[datetime, datetime]]:
    day = _get_calendar_day(target_date)
    if not day or day.get("status") != "open":
        logger.info(service_message("Tradier", "Calendar shows %s is not an open trading session."), target_date)
        return None

    open_info = day.get("open") or {}
    start = open_info.get("start")
    end = open_info.get("end")
    if not start or not end:
        raise TradierError(f"Tradier calendar day {target_date} did not include open session bounds.")
    return _parse_market_time(target_date, start), _parse_market_time(target_date, end)
