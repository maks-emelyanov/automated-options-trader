from typing import Any, Dict

from trading.earnings_trader import AlpacaConfigError, TradierError, get_alpaca_clients, minutes_until_session_close, run_trading_session
from trading.logging_utils import configure_logging, get_logger


logger = get_logger(__name__)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    configure_logging()
    del context
    logger.info("Lambda invocation started for earnings-trader: event=%s", event)

    try:
        get_alpaca_clients()
    except AlpacaConfigError as exc:
        logger.exception("Lambda is missing Alpaca configuration.")
        raise exc

    try:
        minutes_to_close = minutes_until_session_close()
    except TradierError as exc:
        logger.exception("Lambda is missing Tradier configuration.")
        raise exc
    if minutes_to_close is None:
        logger.info("Skipping run because the market is currently closed.")
        return {
            "status": "skipped",
            "reason": "market_closed",
        }

    if not 14.0 <= minutes_to_close <= 16.0:
        logger.info("Skipping run because market close is not 15 minutes away: %.2f", minutes_to_close)
        return {
            "status": "skipped",
            "reason": "outside_close_window",
            "minutes_until_close": round(minutes_to_close, 2),
        }

    result = run_trading_session()
    logger.info("Lambda invocation completed for earnings-trader: submitted_symbol_count=%s", result["submitted_symbol_count"])
    return {
        "status": "completed",
        "minutes_until_close": round(minutes_to_close, 2),
        **result,
    }
