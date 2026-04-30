import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Tuple
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import (
    AssetClass,
    OrderClass,
    OrderSide,
    OrderType,
    PositionIntent,
    TimeInForce,
)
from alpaca.trading.requests import MarketOrderRequest, OptionLegRequest
from trading.logging_utils import (
    configure_logging,
    get_logger,
    log_external_request,
    log_external_response,
    service_message,
    symbol_message,
)
from trading.retry_utils import call_with_retries


logger = get_logger(__name__)

CLOSE_ORDER_SUBMIT_RETRY_ATTEMPTS = int(os.getenv("CLOSE_ORDER_SUBMIT_RETRY_ATTEMPTS", "5"))
AMBIGUOUS_SUBMIT_ERROR_TYPES = {
    "broker_internal_error",
    "rate_limited",
    "unknown_submission_error",
}


class AlpacaConfigError(RuntimeError):
    pass


# =========================
# Data classes
# =========================

@dataclass
class OptionPositionInfo:
    symbol: str
    underlying: str
    contract_type: str   # "call" or "put"
    strike: float
    expiration: date
    side: str            # "long" or "short"
    qty: int


@dataclass
class CalendarPair:
    underlying: str
    contract_type: str
    strike: float
    leg1: OptionPositionInfo
    leg2: OptionPositionInfo
    qty: int


# =========================
# Helpers
# =========================

def get_trade_client() -> TradingClient:
    api_key = os.getenv("ALPACA_API_KEY", "")
    secret_key = os.getenv("ALPACA_SECRET_KEY", "")
    if not api_key or not secret_key:
        raise AlpacaConfigError("Set ALPACA_API_KEY and ALPACA_SECRET_KEY before closing positions.")
    logger.info(service_message("Alpaca", "Initializing trading client for close-options."))
    return TradingClient(api_key, secret_key, paper=True)


def is_dry_run() -> bool:
    return os.getenv("DRY_RUN", "false").lower() == "true"


def enum_value(x) -> str:
    return getattr(x, "value", str(x))


def classify_alpaca_error(exc: Exception) -> str:
    """
    Bucket Alpaca failures into coarse groups so logs distinguish likely
    broker/internal instability from request or account validation issues.
    """
    message = str(exc).lower()
    if "internal server error" in message:
        return "broker_internal_error"
    if "rate limit" in message or "too many requests" in message:
        return "rate_limited"
    if "insufficient" in message or "buying power" in message:
        return "account_constraint"
    if "invalid" in message or "validation" in message:
        return "request_validation_error"
    return "unknown_submission_error"


def serialize_close_request(req: MarketOrderRequest) -> Dict[str, object]:
    """
    Build a log-friendly representation of the outbound MLEG close order.
    """
    legs = []
    for leg in req.legs or []:
        legs.append(
            {
                "symbol": leg.symbol,
                "ratio_qty": leg.ratio_qty,
                "side": enum_value(leg.side),
                "position_intent": enum_value(leg.position_intent),
            }
        )

    return {
        "qty": req.qty,
        "type": enum_value(req.type),
        "time_in_force": enum_value(req.time_in_force),
        "order_class": enum_value(req.order_class),
        "client_order_id": req.client_order_id,
        "legs": legs,
    }


def lookup_order_by_client_order_id(trade_client: TradingClient, client_order_id: str):
    """
    Recover from ambiguous submit failures where Alpaca may have accepted the
    order but returned an error before the response reached us.
    """
    log_external_request(
        logger,
        "Alpaca",
        "get_order_by_client_id",
        fields={"workflow": "close_options", "client_order_id": client_order_id},
    )
    order = call_with_retries(
        lambda: trade_client.get_order_by_client_id(client_order_id),
        service="Alpaca",
        action="get_order_by_client_id",
    )
    log_external_response(
        logger,
        "Alpaca",
        "get_order_by_client_id",
        fields={"workflow": "close_options", "client_order_id": client_order_id},
        details=f"order_id={order.id} status={order.status}",
    )
    return order


def submit_close_order(trade_client: TradingClient, req: MarketOrderRequest, *, underlying: str):
    try:
        return call_with_retries(
            lambda: trade_client.submit_order(req),
            service="Alpaca",
            action="submit_order",
            attempts=CLOSE_ORDER_SUBMIT_RETRY_ATTEMPTS,
        )
    except Exception as exc:
        error_type = classify_alpaca_error(exc)
        if error_type not in AMBIGUOUS_SUBMIT_ERROR_TYPES:
            raise

        logger.warning(
            symbol_message(
                underlying,
                "Submit failed with ambiguous broker response; checking for existing order by client_order_id=%s before marking failed.",
            ),
            req.client_order_id,
        )
        try:
            order = lookup_order_by_client_order_id(trade_client, req.client_order_id)
        except Exception as lookup_exc:
            logger.warning(
                symbol_message(
                    underlying,
                    "No recoverable order found after ambiguous submit failure: client_order_id=%s lookup_error=%s",
                ),
                req.client_order_id,
                lookup_exc,
            )
            raise exc from lookup_exc

        logger.info(
            symbol_message(
                underlying,
                "Recovered submitted order after ambiguous broker response: order_id=%s status=%s",
            ),
            order.id,
            order.status,
        )
        return order


def get_position_qty_available(position) -> int:
    """
    Prefer qty_available if present so we do not try to close contracts already tied up
    in other open orders.
    """
    raw = getattr(position, "qty_available", None)
    if raw is None:
        raw = getattr(position, "qty", "0")
    return int(abs(float(raw)))


def load_open_option_positions() -> List[OptionPositionInfo]:
    """
    Pull all open positions, keep only US options, and enrich them with option contract metadata.
    """
    trade_client = get_trade_client()
    log_external_request(logger, "Alpaca", "get_all_positions", fields={"workflow": "close_options"})
    positions = call_with_retries(
        trade_client.get_all_positions,
        service="Alpaca",
        action="get_all_positions",
    )
    log_external_response(
        logger,
        "Alpaca",
        "get_all_positions",
        fields={"workflow": "close_options"},
        details=f"positions={len(positions)}",
    )
    option_positions: List[OptionPositionInfo] = []

    for position in positions:
        asset_class = enum_value(getattr(position, "asset_class", ""))
        if asset_class != AssetClass.US_OPTION.value:
            continue

        qty = get_position_qty_available(position)
        if qty < 1:
            continue

        side = enum_value(getattr(position, "side", "")).lower()
        if side not in {"long", "short"}:
            continue

        symbol = position.symbol
        log_external_request(logger, "Alpaca", "get_option_contract", fields={"symbol": symbol})
        contract = call_with_retries(
            lambda: trade_client.get_option_contract(symbol),
            service="Alpaca",
            action="get_option_contract",
        )
        log_external_response(
            logger,
            "Alpaca",
            "get_option_contract",
            fields={"symbol": symbol},
            details=f"underlying={contract.underlying_symbol} expiration={contract.expiration_date}",
        )

        option_positions.append(
            OptionPositionInfo(
                symbol=symbol,
                underlying=contract.underlying_symbol,
                contract_type=enum_value(contract.type).lower(),
                strike=float(contract.strike_price),
                expiration=contract.expiration_date,
                side=side,
                qty=qty,
            )
        )

    logger.info(service_message("Workflow", "Identified %s closeable option positions for close-options."), len(option_positions))
    return option_positions


def build_calendar_pairs(option_positions: List[OptionPositionInfo]) -> List[CalendarPair]:
    """
    Finds matched calendar spreads by grouping open options by:
      underlying + option type + strike

    Then pairs opposite-side positions with DIFFERENT expirations.
    This will match both:
      - long calendars (near short, far long)
      - short calendars (near long, far short)

    If quantities differ, it closes only the matched portion and leaves extra orphan legs alone.
    """
    grouped: Dict[Tuple[str, str, float], Dict[str, List[dict]]] = defaultdict(
        lambda: {"long": [], "short": []}
    )

    for pos in option_positions:
        key = (pos.underlying, pos.contract_type, pos.strike)
        grouped[key][pos.side].append({"pos": pos, "remaining": pos.qty})

    pairs: List[CalendarPair] = []
    logger.info(service_message("Workflow", "Grouping %s option positions into potential calendar spreads."), len(option_positions))

    for (underlying, contract_type, strike), bucket in grouped.items():
        longs = bucket["long"]
        shorts = bucket["short"]

        while True:
            candidates = []

            for i, long_entry in enumerate(longs):
                if long_entry["remaining"] <= 0:
                    continue

                for j, short_entry in enumerate(shorts):
                    if short_entry["remaining"] <= 0:
                        continue

                    long_pos = long_entry["pos"]
                    short_pos = short_entry["pos"]

                    # Same expiration = not a calendar spread
                    if long_pos.expiration == short_pos.expiration:
                        continue

                    exp_gap = abs((long_pos.expiration - short_pos.expiration).days)

                    # Prefer the closest expiration pair first if multiple exist
                    candidates.append((exp_gap, i, j))

            if not candidates:
                break

            _, i, j = min(candidates, key=lambda x: x[0])

            long_entry = longs[i]
            short_entry = shorts[j]

            qty = min(long_entry["remaining"], short_entry["remaining"])
            if qty <= 0:
                break

            pairs.append(
                CalendarPair(
                    underlying=underlying,
                    contract_type=contract_type,
                    strike=strike,
                    leg1=long_entry["pos"],
                    leg2=short_entry["pos"],
                    qty=qty,
                )
            )
            logger.info(
                service_message("Workflow", "Matched calendar spread: underlying=%s type=%s strike=%.2f qty=%s expirations=%s/%s"),
                underlying,
                contract_type,
                strike,
                qty,
                long_entry["pos"].expiration,
                short_entry["pos"].expiration,
            )

            long_entry["remaining"] -= qty
            short_entry["remaining"] -= qty

    logger.info(service_message("Workflow", "Built %s calendar spread pairs."), len(pairs))
    return pairs


def leg_close_order_fields(pos: OptionPositionInfo):
    """
    Convert an existing position into the correct close instruction.
    """
    if pos.side == "long":
        return OrderSide.SELL, PositionIntent.SELL_TO_CLOSE
    elif pos.side == "short":
        return OrderSide.BUY, PositionIntent.BUY_TO_CLOSE
    raise ValueError(f"Unsupported position side: {pos.side}")


def make_close_request(pair: CalendarPair) -> MarketOrderRequest:
    """
    Submit a 2-leg market MLeg order that closes both sides together.
    """
    leg1_side, leg1_intent = leg_close_order_fields(pair.leg1)
    leg2_side, leg2_intent = leg_close_order_fields(pair.leg2)

    client_order_id = (
        f"close-cal-{pair.underlying.lower()}-"
        f"{pair.contract_type[0]}-"
        f"{int(pair.strike * 1000)}-"
        f"{pair.leg1.expiration:%Y%m%d}-"
        f"{pair.leg2.expiration:%Y%m%d}"
    )[:48]

    return MarketOrderRequest(
        qty=pair.qty,
        type=OrderType.MARKET,
        time_in_force=TimeInForce.DAY,
        order_class=OrderClass.MLEG,
        client_order_id=client_order_id,
        legs=[
            OptionLegRequest(
                symbol=pair.leg1.symbol,
                ratio_qty=1,
                side=leg1_side,
                position_intent=leg1_intent,
            ),
            OptionLegRequest(
                symbol=pair.leg2.symbol,
                ratio_qty=1,
                side=leg2_side,
                position_intent=leg2_intent,
            ),
        ],
    )


def describe_pair(pair: CalendarPair) -> str:
    near = pair.leg1 if pair.leg1.expiration < pair.leg2.expiration else pair.leg2
    far = pair.leg2 if near is pair.leg1 else pair.leg1

    spread_kind = "long calendar" if near.side == "short" and far.side == "long" else "short calendar"

    return (
        f"{pair.underlying} {pair.contract_type.upper()} strike {pair.strike:.2f} "
        f"| near {near.expiration} ({near.side} {near.symbol}) "
        f"| far {far.expiration} ({far.side} {far.symbol}) "
        f"| qty {pair.qty} | detected as {spread_kind}"
    )


def close_open_calendar_spreads() -> Dict[str, object]:
    configure_logging()
    trade_client = get_trade_client()
    logger.info(service_message("Workflow", "Starting end-to-end close-options session."))
    option_positions = load_open_option_positions()

    if not option_positions:
        logger.info(service_message("Workflow", "No open option positions found; skipping close-options order submission."))
        return {
            "status": "completed",
            "detected_spread_count": 0,
            "submitted_order_count": 0,
            "failed_order_count": 0,
            "dry_run": is_dry_run(),
        }

    pairs = build_calendar_pairs(option_positions)

    if not pairs:
        logger.info(service_message("Workflow", "No open calendar spreads detected; skipping close-options order submission."))
        return {
            "status": "completed",
            "detected_spread_count": 0,
            "submitted_order_count": 0,
            "failed_order_count": 0,
            "dry_run": is_dry_run(),
        }

    logger.info(service_message("Workflow", "Proceeding to close %s detected calendar spread(s)."), len(pairs))
    for pair in pairs:
        logger.info(service_message("Workflow", "Calendar spread candidate: %s"), describe_pair(pair))

    submitted_order_count = 0
    failed_order_count = 0
    dry_run = is_dry_run()

    for pair in pairs:
        req = make_close_request(pair)

        if dry_run:
            logger.info(symbol_message(pair.underlying, "DRY RUN enabled; order not submitted."))
            continue

        try:
            request_payload = serialize_close_request(req)
            log_external_request(
                logger,
                "Alpaca",
                "submit_order",
                fields={
                    "workflow": "close_options",
                    "client_order_id": req.client_order_id,
                    "underlying": pair.underlying,
                    "request": request_payload,
                },
            )
            order = submit_close_order(trade_client, req, underlying=pair.underlying)
            log_external_response(
                logger,
                "Alpaca",
                "submit_order",
                fields={"workflow": "close_options", "client_order_id": req.client_order_id, "underlying": pair.underlying},
                details=f"order_id={order.id} status={order.status}",
            )
            logger.info(symbol_message(pair.underlying, "Order submitted successfully: order_id=%s status=%s"), order.id, order.status)
            submitted_order_count += 1
        except Exception as exc:
            logger.error(
                symbol_message(
                    pair.underlying,
                    "Skipped due to error while closing spread: error_type=%s client_order_id=%s request=%s error=%s",
                ),
                classify_alpaca_error(exc),
                req.client_order_id,
                request_payload,
                exc,
            )
            failed_order_count += 1

    logger.info(
        service_message("Workflow", "Close-options session completed: detected_spread_count=%s submitted_order_count=%s failed_order_count=%s dry_run=%s"),
        len(pairs),
        submitted_order_count,
        failed_order_count,
        dry_run,
    )

    return {
        "status": "completed",
        "detected_spread_count": len(pairs),
        "submitted_order_count": submitted_order_count,
        "failed_order_count": failed_order_count,
        "dry_run": dry_run,
    }


if __name__ == "__main__":
    configure_logging()
    close_open_calendar_spreads()
