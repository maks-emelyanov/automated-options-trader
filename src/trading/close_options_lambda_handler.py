import os
from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from trading.close_options import AlpacaConfigError, close_open_calendar_spreads, get_trade_client
from trading.earnings_trader import TradierError, get_tradier_market_clock, get_tradier_session_window
from trading.logging_utils import configure_logging, get_logger, service_message


logger = get_logger(__name__)


def minutes_since_session_open() -> Optional[float]:
    market_tz = ZoneInfo(os.getenv("MARKET_TIMEZONE", "America/New_York"))
    logger.info(service_message("Tradier", "Requesting session window to calculate minutes since session open."))
    clock = get_tradier_market_clock()
    if clock.get("state") != "open":
        logger.info(service_message("Tradier", "Market clock indicates the market is not open."))
        return None

    session_now = datetime.fromtimestamp(int(clock["timestamp"]), tz=market_tz)

    session_window = get_tradier_session_window(session_now.date())
    if session_window is None:
        logger.info(service_message("Tradier", "Session window lookup found no open market session for %s."), session_now.date())
        return None
    open_time, _ = session_window
    minutes_since_open = (session_now - open_time).total_seconds() / 60.0
    logger.info(service_message("Tradier", "Session window indicates %.2f minutes since session open."), minutes_since_open)
    return minutes_since_open


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    configure_logging()
    del event
    del context
    logger.info(service_message("Lambda", "Invocation started for close-options."))

    try:
        get_trade_client()
    except AlpacaConfigError as exc:
        logger.error(service_message("Alpaca", "Lambda is missing configuration: %s"), exc)
        raise exc

    try:
        minutes_from_open = minutes_since_session_open()
    except TradierError as exc:
        logger.error(service_message("Tradier", "Lambda is missing configuration: %s"), exc)
        raise exc
    if minutes_from_open is None:
        logger.info(service_message("Lambda", "Skipping run because the market is currently closed."))
        return {
            "status": "skipped",
            "reason": "market_closed",
        }

    if not 14.0 <= minutes_from_open <= 16.0:
        logger.info(service_message("Lambda", "Skipping run because market open is not 15 minutes ago: %.2f"), minutes_from_open)
        return {
            "status": "skipped",
            "reason": "outside_open_window",
            "minutes_since_open": round(minutes_from_open, 2),
        }

    result = close_open_calendar_spreads()
    logger.info(
        service_message("Lambda", "Invocation completed for close-options: detected_spread_count=%s submitted_order_count=%s"),
        result["detected_spread_count"],
        result["submitted_order_count"],
    )
    return {
        "status": "completed",
        "minutes_since_open": round(minutes_from_open, 2),
        **result,
    }
