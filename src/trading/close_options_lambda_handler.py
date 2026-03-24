import os
from datetime import datetime, time
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from trading.close_options import AlpacaConfigError, close_open_calendar_spreads, get_trade_client
from trading.logging_utils import configure_logging, get_logger


logger = get_logger(__name__)


def minutes_since_session_open(trade_client) -> Optional[float]:
    logger.info("Requesting Alpaca market clock to calculate minutes since session open.")
    clock = trade_client.get_clock()
    if not getattr(clock, "is_open", False):
        logger.info("Alpaca clock indicates the market is closed.")
        return None

    now = clock.timestamp
    if now.tzinfo is None:
        now = now.replace(tzinfo=ZoneInfo(os.getenv("MARKET_TIMEZONE", "America/New_York")))

    market_tz = ZoneInfo(os.getenv("MARKET_TIMEZONE", "America/New_York"))
    session_now = now.astimezone(market_tz)
    open_time = datetime.combine(session_now.date(), time(hour=9, minute=30), tzinfo=market_tz)
    minutes_since_open = (session_now - open_time).total_seconds() / 60.0
    logger.info("Alpaca clock indicates %.2f minutes since session open.", minutes_since_open)
    return minutes_since_open


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    configure_logging()
    del event
    del context
    logger.info("Lambda invocation started for close-options.")

    try:
        trade_client = get_trade_client()
    except AlpacaConfigError as exc:
        logger.exception("Lambda is missing Alpaca configuration.")
        raise exc

    minutes_from_open = minutes_since_session_open(trade_client)
    if minutes_from_open is None:
        logger.info("Skipping run because the market is currently closed.")
        return {
            "status": "skipped",
            "reason": "market_closed",
        }

    if not 14.0 <= minutes_from_open <= 16.0:
        logger.info("Skipping run because market open is not 15 minutes ago: %.2f", minutes_from_open)
        return {
            "status": "skipped",
            "reason": "outside_open_window",
            "minutes_since_open": round(minutes_from_open, 2),
        }

    result = close_open_calendar_spreads()
    logger.info(
        "Lambda invocation completed for close-options: detected_spread_count=%s submitted_order_count=%s",
        result["detected_spread_count"],
        result["submitted_order_count"],
    )
    return {
        "status": "completed",
        "minutes_since_open": round(minutes_from_open, 2),
        **result,
    }
