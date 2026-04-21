import os
import asyncio
import math
import aiohttp
import pandas as pd
import numpy as np
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple, Iterable
import io
import csv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    OptionLegRequest,
)
from alpaca.trading.enums import (
    OrderClass,
    OrderSide,
    OrderType,
    TimeInForce,
)
from trading.logging_utils import (
    configure_logging,
    get_logger,
    log_external_request,
    log_external_response,
    service_message,
    symbol_message,
    service_symbol_message,
)
from trading.retry_utils import async_call_with_retries, call_with_retries


logger = get_logger(__name__)

# -----------------------------
# Config
# -----------------------------
TRADIER_BASE_URL = "https://api.tradier.com"
TRADIER_TOKEN = os.getenv("TRADIER_TOKEN", "")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "")
ALPHAVANTAGE_URL = "https://www.alphavantage.co/query"
ALPHAVANTAGE_FN = "EARNINGS_CALENDAR"
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")

PCT_OF_AVAILABLE = float(os.getenv("PCT_OF_AVAILABLE", "0.06"))
ACCOUNT_VALUE_FIELD = os.getenv("ACCOUNT_VALUE_FIELD", "cash")
BUDGET_MODE = os.getenv("BUDGET_MODE", "per_symbol")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
STRIKE_WINDOW_PCT = float(os.getenv("STRIKE_WINDOW_PCT", "0.10"))
USE_MID_DEBIT = os.getenv("USE_MID_DEBIT", "false").lower() == "true"
MIN_NET_DEBIT = float(os.getenv("MIN_NET_DEBIT", "0.01"))
MARKET_ORDER_SLIPPAGE_PCT = float(os.getenv("MARKET_ORDER_SLIPPAGE_PCT", "0.10"))
MARKET_TIMEZONE = os.getenv("MARKET_TIMEZONE", "America/New_York")

class AlphaVantageError(RuntimeError):
    ...


class TradierError(RuntimeError):
    ...


class AlpacaConfigError(RuntimeError):
    ...


@dataclass
class SpreadCandidate:
    underlying: str
    spot: float
    strike: float
    short_exp: date
    long_exp: date
    short_symbol: str
    long_symbol: str
    short_bid: float
    short_ask: float
    long_bid: float
    long_ask: float
    natural_debit: float
    mid_debit: float


@dataclass
class TradierOptionContract:
    symbol: str
    expiration_date: date
    strike_price: float
    bid: float
    ask: float


# -----------------------------
# Small utilities
# -----------------------------
def _as_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _log_http_request(service: str, url: str, *, params: Optional[Dict[str, Any]] = None) -> None:
    log_external_request(logger, service, "GET", fields={"url": url, **(params or {})})


def _log_http_response(
    service: str,
    url: str,
    *,
    status: int,
    params: Optional[Dict[str, Any]] = None,
    details: Optional[str] = None,
) -> None:
    log_external_response(
        logger,
        service,
        "GET",
        fields={"url": url, "status": status, **(params or {})},
        details=details,
    )


def filter_dates(dates: List[str]) -> List[str]:
    today = datetime.today().date()
    cutoff_date = today + timedelta(days=45)
    sorted_dates = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in dates)
    arr = []
    for i, d in enumerate(sorted_dates):
        if d >= cutoff_date:
            arr = [x.strftime("%Y-%m-%d") for x in sorted_dates[:i+1]]
            break
    if arr:
        if arr[0] == today.strftime("%Y-%m-%d"):
            return arr[1:]
        return arr
    raise ValueError("No date 45 days or more in the future found.")


def yang_zhang(price_data: pd.DataFrame, window=30, trading_periods=252, return_last_only=True):
    log_ho = (price_data['High'] / price_data['Open']).apply(np.log)
    log_lo = (price_data['Low'] / price_data['Open']).apply(np.log)
    log_co = (price_data['Close'] / price_data['Open']).apply(np.log)

    log_oc = (price_data['Open'] / price_data['Close'].shift(1)).apply(np.log)
    log_oc_sq = log_oc**2
    log_cc = (price_data['Close'] / price_data['Close'].shift(1)).apply(np.log)
    log_cc_sq = log_cc**2

    rs = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)

    close_vol = log_cc_sq.rolling(window=window).sum() * (1.0 / (window - 1.0))
    open_vol = log_oc_sq.rolling(window=window).sum() * (1.0 / (window - 1.0))
    window_rs = rs.rolling(window=window).sum() * (1.0 / (window - 1.0))

    k = 0.34 / (1.34 + ((window + 1) / (window - 1)))
    result = (open_vol + k * close_vol + (1 - k) * window_rs).apply(np.sqrt) * np.sqrt(trading_periods)
    return result.iloc[-1] if return_last_only else result.dropna()


def build_term_structure(days, ivs):
    days = np.array(days, dtype=float)
    ivs = np.array(ivs, dtype=float)
    idx = days.argsort()
    days = days[idx]
    ivs = ivs[idx]

    def term_spline(dte):
        if dte < days[0]:
            return float(ivs[0])
        if dte > days[-1]:
            return float(ivs[-1])
        return float(np.interp(dte, days, ivs))

    return term_spline


# -----------------------------
# S&P 500 helpers
# -----------------------------
def _normalize_symbol(sym: str) -> str:
    """
    Normalize ticker symbols so that variants like 'BRK.B' and 'BRK-B'
    compare consistently.
    """
    return (sym or "").strip().upper().replace(".", "-")


def get_sp500_tickers() -> set[str]:
    """
    Fetch the current S&P 500 constituents from a public CSV dataset
    and return a set of normalized ticker symbols.
    """
    url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    logger.info(service_message("DataHub", "Loading S&P 500 constituents from %s"), url)

    df = pd.read_csv(url)

    if "Symbol" not in df.columns:
        raise RuntimeError(
            f"Could not find 'Symbol' column in S&P 500 CSV. Got columns: {list(df.columns)}"
        )

    symbols = (
        df["Symbol"]
        .astype(str)
        .map(_normalize_symbol)
    )
    logger.info(service_message("DataHub", "Loaded %s normalized S&P 500 symbols."), len(symbols))
    return set(symbols)


def filter_symbols_to_sp500(symbols: Iterable[str], sp500: set[str]) -> List[str]:
    """
    Return unique symbols that are members of the S&P 500, preserving input order.
    """
    candidates = list(symbols)
    filtered: List[str] = []
    seen: set[str] = set()
    for sym in candidates:
        normalized = _normalize_symbol(sym)
        if normalized in sp500 and normalized not in seen:
            filtered.append(sym)
            seen.add(normalized)
    logger.info(
        service_message("DataHub", "Filtering %s candidate symbols against %s S&P 500 symbols yielded %s matches."),
        len(candidates),
        len(sp500),
        len(filtered),
    )
    return filtered


# -----------------------------
# HTTP helpers (aiohttp)
# -----------------------------
def _tradier_headers() -> Dict[str, str]:
    if not TRADIER_TOKEN:
        raise TradierError("TRADIER_TOKEN environment variable is not set.")
    return {"Authorization": f"Bearer {TRADIER_TOKEN}", "Accept": "application/json"}


def _market_timezone() -> ZoneInfo:
    return ZoneInfo(MARKET_TIMEZONE)


def _parse_market_time(session_date: date, hhmm: str) -> datetime:
    hour_str, minute_str = (hhmm or "").split(":", 1)
    return datetime.combine(
        session_date,
        datetime.min.time().replace(hour=int(hour_str), minute=int(minute_str)),
        tzinfo=_market_timezone(),
    )


async def _aio_get_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params=None,
    headers=None,
    timeout: float = 20.0,
) -> Any:
    async def _request() -> Any:
        _log_http_request("Tradier", url, params=params)
        try:
            async with session.get(url, params=params, headers=headers, timeout=timeout) as r:
                if r.status == 429:
                    _log_http_response("Tradier", url, status=r.status, params=params, details="rate_limited")
                    raise RuntimeError("API rate limit reached (HTTP 429).")
                r.raise_for_status()
                payload = await r.json()
                _log_http_response("Tradier", url, status=r.status, params=params)
                return payload
        except aiohttp.ClientResponseError as e:
            logger.warning(service_message("Tradier", "Request failed: url=%s status=%s message=%s"), url, e.status, e.message)
            raise RuntimeError(f"HTTP error {e.status}: {e.message}") from e
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
            logger.warning(service_message("Tradier", "Network request failed: url=%s error=%s"), url, e)
            raise RuntimeError(f"Network error: {e}") from e

    return await async_call_with_retries(
        _request,
        service="Tradier",
        action="GET",
    )


async def _get_market_clock(session: aiohttp.ClientSession) -> Dict[str, Any]:
    logger.info(service_message("Tradier", "Requesting market clock."))
    data = await _aio_get_json(
        session,
        f"{TRADIER_BASE_URL}/v1/markets/clock",
        headers=_tradier_headers(),
    )
    clock = data.get("clock") or {}
    if not clock:
        raise TradierError("Tradier market clock response did not include clock data.")
    logger.info(
        service_message("Tradier", "Fetched market clock: state=%s next_state=%s"),
        clock.get("state"),
        clock.get("next_state"),
    )
    return clock


def get_tradier_market_clock() -> Dict[str, Any]:
    async def _fetch() -> Dict[str, Any]:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            return await _get_market_clock(session)

    return asyncio.run(_fetch())


async def _get_market_calendar_days(
    session: aiohttp.ClientSession,
    *,
    year: int,
    month: int,
    cache: Optional[Dict[Tuple[int, int], List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    cache_key = (year, month)
    if cache is not None and cache_key in cache:
        logger.info(service_message("Tradier", "Using cached market calendar rows for year=%s month=%s."), year, month)
        return cache[cache_key]

    logger.info(service_message("Tradier", "Requesting market calendar for year=%s month=%s."), year, month)
    data = await _aio_get_json(
        session,
        f"{TRADIER_BASE_URL}/v1/markets/calendar",
        params={"year": str(year), "month": f"{month:02d}"},
        headers=_tradier_headers(),
    )
    days = (data.get("calendar") or {}).get("days", {}).get("day", [])
    if isinstance(days, dict):
        days = [days]
    logger.info(service_message("Tradier", "Fetched %s market calendar day rows for year=%s month=%s."), len(days), year, month)
    if cache is not None:
        cache[cache_key] = days
    return days


async def _get_calendar_day(
    session: aiohttp.ClientSession,
    target_date: date,
    cache: Optional[Dict[Tuple[int, int], List[Dict[str, Any]]]] = None,
) -> Optional[Dict[str, Any]]:
    days = await _get_market_calendar_days(
        session,
        year=target_date.year,
        month=target_date.month,
        cache=cache,
    )
    target_iso = target_date.isoformat()
    for day in days:
        if day.get("date") == target_iso:
            return day
    return None


async def _fetch_tradier_session_window(
    target_date: date,
) -> Optional[Tuple[datetime, datetime]]:
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        calendar_cache: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
        day = await _get_calendar_day(session, target_date, cache=calendar_cache)
        if not day or day.get("status") != "open":
            logger.info(service_message("Tradier", "Calendar shows %s is not an open trading session."), target_date)
            return None
        open_info = day.get("open") or {}
        start = open_info.get("start")
        end = open_info.get("end")
        if not start or not end:
            raise TradierError(f"Tradier calendar day {target_date} did not include open session bounds.")
        return _parse_market_time(target_date, start), _parse_market_time(target_date, end)


def get_tradier_session_window(target_date: date) -> Optional[Tuple[datetime, datetime]]:
    return asyncio.run(_fetch_tradier_session_window(target_date))


async def _fetch_minutes_until_session_close() -> Optional[float]:
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        calendar_cache: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
        clock = await _get_market_clock(session)
        if clock.get("state") != "open":
            logger.info(service_message("Tradier", "Market clock indicates the market is not open."))
            return None

        session_date = datetime.strptime(clock["date"], "%Y-%m-%d").date()
        day = await _get_calendar_day(session, session_date, cache=calendar_cache)
        if not day or day.get("status") != "open":
            logger.info(service_message("Tradier", "Calendar shows no open session for %s."), session_date)
            return None
        close_time = ((day.get("open") or {}).get("end"))
        if not close_time:
            raise TradierError(f"Tradier calendar day {session_date} did not include a close time.")

        now = datetime.fromtimestamp(int(clock["timestamp"]), tz=_market_timezone())
        close_dt = _parse_market_time(session_date, close_time)
        minutes_remaining = (close_dt - now).total_seconds() / 60.0
        logger.info(service_message("Tradier", "Clock indicates %.2f minutes until session close."), minutes_remaining)
        return minutes_remaining


def minutes_until_session_close() -> Optional[float]:
    return asyncio.run(_fetch_minutes_until_session_close())


async def _fetch_next_trading_session_date() -> date:
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        calendar_cache: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
        clock = await _get_market_clock(session)
        now = datetime.fromtimestamp(int(clock["timestamp"]), tz=_market_timezone())
        session_date = datetime.strptime(clock["date"], "%Y-%m-%d").date()
        current_day = await _get_calendar_day(session, session_date, cache=calendar_cache)

        if current_day and current_day.get("status") == "open":
            open_start = ((current_day.get("open") or {}).get("start"))
            if not open_start:
                raise TradierError(f"Tradier calendar day {session_date} did not include an open time.")
            if now < _parse_market_time(session_date, open_start):
                logger.info(service_message("Tradier", "Next trading session date resolved to %s."), session_date)
                return session_date

        search_date = session_date + timedelta(days=1)
        for _ in range(370):
            day = await _get_calendar_day(session, search_date, cache=calendar_cache)
            if day and day.get("status") == "open":
                logger.info(service_message("Tradier", "Next trading session date resolved to %s."), search_date)
                return search_date
            search_date += timedelta(days=1)

    raise TradierError("Could not determine the next trading session date from Tradier calendar data.")


# -----------------------------
# Tradier (async)
# -----------------------------
async def get_current_price(session: aiohttp.ClientSession, symbol: str) -> float:
    logger.info(service_symbol_message("Tradier", symbol, "Fetching current price."))
    data = await _aio_get_json(
        session,
        f"{TRADIER_BASE_URL}/v1/markets/quotes",
        params={"symbols": symbol},
        headers=_tradier_headers(),
    )
    quote = data.get("quotes", {}).get("quote")
    if quote is None:
        raise ValueError("No quote data.")
    if isinstance(quote, list):
        quote = quote[0]
    last = _as_float(quote.get("last")) or _as_float(quote.get("close"))
    if last is None:
        raise ValueError("No market price.")
    logger.info(service_symbol_message("Tradier", symbol, "Fetched current price: %.4f"), last)
    return last


async def get_price_history(
    session: aiohttp.ClientSession, symbol: str, start_date: datetime, end_date: datetime
) -> pd.DataFrame:
    logger.info(
        service_symbol_message("Tradier", symbol, "Fetching price history from %s to %s."),
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
    data = await _aio_get_json(
        session,
        f"{TRADIER_BASE_URL}/v1/markets/history",
        params={
            "symbol": symbol,
            "start": start_date.strftime("%Y-%m-%d"),
            "end": end_date.strftime("%Y-%m-%d"),
        },
        headers=_tradier_headers(),
    )
    days = data.get("history", {}).get("day", [])
    if not days:
        raise ValueError("No historical data.")
    if isinstance(days, dict):
        days = [days]
    df = pd.DataFrame(
        [
            {
                "Date": d.get("date"),
                "Open": _as_float(d.get("open")),
                "High": _as_float(d.get("high")),
                "Low": _as_float(d.get("low")),
                "Close": _as_float(d.get("close")),
                "Volume": _as_float(d.get("volume")),
            }
            for d in days
        ]
    )
    cleaned = df.dropna(subset=["Open", "High", "Low", "Close"]).reset_index(drop=True)
    logger.info(service_symbol_message("Tradier", symbol, "Fetched %s cleaned historical price rows."), len(cleaned))
    return cleaned


async def get_expirations(session: aiohttp.ClientSession, symbol: str) -> List[str]:
    logger.info(service_symbol_message("Tradier", symbol, "Fetching option expirations."))
    data = await _aio_get_json(
        session,
        f"{TRADIER_BASE_URL}/v1/markets/options/expirations",
        params={"symbol": symbol, "includeAllRoots": "true", "strikes": "false"},
        headers=_tradier_headers(),
    )
    exps = data.get("expirations", {}).get("date", [])
    if not exps:
        logger.info(service_symbol_message("Tradier", symbol, "No option expirations returned."))
        return []
    expirations = [exps] if isinstance(exps, str) else exps
    logger.info(service_symbol_message("Tradier", symbol, "Fetched %s expirations."), len(expirations))
    return expirations


async def get_option_chain(
    session: aiohttp.ClientSession, symbol: str, expiration: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(service_symbol_message("Tradier", symbol, "Fetching option chain for expiration %s."), expiration)
    data = await _aio_get_json(
        session,
        f"{TRADIER_BASE_URL}/v1/markets/options/chains",
        params={"symbol": symbol, "expiration": expiration, "greeks": "true"},
        headers=_tradier_headers(),
    )
    options = data.get("options", {}).get("option", [])
    if not options:
        empty = pd.DataFrame(columns=["strike", "bid", "ask", "impliedVolatility"])
        logger.info(service_symbol_message("Tradier", symbol, "No option chain rows returned for expiration %s."), expiration)
        return empty, empty
    if isinstance(options, dict):
        options = [options]
    rows = []
    for opt in options:
        g = opt.get("greeks", {}) or {}
        iv = (
            _as_float(g.get("mid_iv"))
            or (lambda b, a: (b + a) / 2.0 if (b is not None and a is not None) else None)(
                _as_float(g.get("bid_iv")), _as_float(g.get("ask_iv"))
            )
            or _as_float(g.get("smv_vol"))
        )
        rows.append(
            {
                "symbol": opt.get("symbol"),
                "expiration": opt.get("expiration_date") or expiration,
                "type": opt.get("option_type"),
                "strike": _as_float(opt.get("strike")),
                "bid": _as_float(opt.get("bid")),
                "ask": _as_float(opt.get("ask")),
                "impliedVolatility": iv,
            }
        )
    df = pd.DataFrame(rows).dropna(subset=["strike", "bid", "ask"])
    calls = df[df["type"] == "call"][
        ["symbol", "expiration", "strike", "bid", "ask", "impliedVolatility"]
    ].reset_index(
        drop=True
    )
    puts = df[df["type"] == "put"][
        ["symbol", "expiration", "strike", "bid", "ask", "impliedVolatility"]
    ].reset_index(
        drop=True
    )
    logger.info(
        service_symbol_message("Tradier", symbol, "Fetched option chain for expiration %s: calls=%s puts=%s"),
        expiration,
        len(calls),
        len(puts),
    )
    return calls, puts


# -----------------------------
# Recommendation logic (async)
# -----------------------------
async def recommend_ticker(session: aiohttp.ClientSession, ticker: str) -> str:
    normalized_ticker = (ticker or "").strip().upper()
    try:
        symbol = normalized_ticker
        if not symbol:
            logger.info(service_message("Workflow", "Received blank ticker; defaulting recommendation to Avoid."))
            return "Avoid"

        logger.info(symbol_message(symbol, "Starting recommendation analysis."))
        exps = await get_expirations(session, symbol)
        if not exps:
            logger.info(symbol_message(symbol, "Recommendation Avoid: no expirations available."))
            return "Avoid"
        try:
            exps = filter_dates(exps)
            logger.info(symbol_message(symbol, "Filtered expirations down to %s target dates."), len(exps))
        except Exception:
            logger.info(symbol_message(symbol, "Recommendation Avoid: expirations did not meet the date filter."))
            return "Avoid"

        try:
            underlying = await get_current_price(session, symbol)
        except Exception:
            logger.info(symbol_message(symbol, "Recommendation Avoid: unable to fetch current price."))
            return "Avoid"

        atm_iv_by_exp = {}
        for i, exp in enumerate(exps):
            calls, puts = await get_option_chain(session, symbol, exp)
            if calls.empty or puts.empty:
                logger.info(symbol_message(symbol, "Skipping expiration %s because the call or put chain is empty."), exp)
                continue
            call_idx = (calls["strike"] - underlying).abs().idxmin()
            put_idx = (puts["strike"] - underlying).abs().idxmin()
            call_iv = calls.loc[call_idx, "impliedVolatility"]
            put_iv = puts.loc[put_idx, "impliedVolatility"]

            if pd.isna(call_iv) and pd.isna(put_iv):
                continue
            elif pd.isna(call_iv):
                atm_iv = put_iv
            elif pd.isna(put_iv):
                atm_iv = call_iv
            else:
                atm_iv = (call_iv + put_iv) / 2.0
            atm_iv_by_exp[exp] = atm_iv

        if not atm_iv_by_exp:
            logger.info(symbol_message(symbol, "Recommendation Avoid: no ATM implied volatility values available."))
            return "Avoid"

        today = datetime.today().date()
        dtes, ivs = [], []
        for exp, iv in atm_iv_by_exp.items():
            dte = (datetime.strptime(exp, "%Y-%m-%d").date() - today).days
            dtes.append(dte)
            ivs.append(iv)
        term = build_term_structure(dtes, ivs)
        ts_slope_0_45 = (term(45) - term(dtes[0])) / (45 - dtes[0])

        end = datetime.today()
        start = end - timedelta(days=100)
        hist = await get_price_history(session, symbol, start, end)
        if hist.empty or len(hist) < 31:
            logger.info(symbol_message(symbol, "Recommendation Avoid: insufficient history rows=%s."), len(hist))
            return "Avoid"

        iv30_rv30 = term(30) / yang_zhang(hist)
        avg_vol_ok = hist["Volume"].rolling(30).mean().dropna()
        if avg_vol_ok.empty:
            logger.info(symbol_message(symbol, "Recommendation Avoid: average volume series is empty."))
            return "Avoid"
        avg_volume_pass = avg_vol_ok.iloc[-1] >= 1_500_000

        iv30_rv30_pass = iv30_rv30 >= 1.25
        ts_slope_pass = ts_slope_0_45 <= -0.00406

        logger.info(
            symbol_message(symbol, "Metrics: iv30_rv30=%.4f pass=%s avg_volume=%.2f pass=%s ts_slope_0_45=%.6f pass=%s"),
            iv30_rv30,
            iv30_rv30_pass,
            avg_vol_ok.iloc[-1],
            avg_volume_pass,
            ts_slope_0_45,
            ts_slope_pass,
        )
        if avg_volume_pass and iv30_rv30_pass and ts_slope_pass:
            logger.info(symbol_message(symbol, "Recommendation result: Recommended."))
            return "Recommended"
        if ts_slope_pass and (
            (avg_volume_pass and not iv30_rv30_pass)
            or (iv30_rv30_pass and not avg_volume_pass)
        ):
            logger.info(symbol_message(symbol, "Recommendation result: Consider."))
            return "Consider"
        logger.info(symbol_message(symbol, "Recommendation result: Avoid."))
        return "Avoid"
    except Exception as exc:
        logger.error(symbol_message(normalized_ticker or "UNKNOWN", "Recommendation failed unexpectedly; defaulting to Avoid: %s"), exc)
        return "Avoid"


# -----------------------------
# Alpha Vantage calendar (async)
# -----------------------------
def _parse_av_json(data: dict) -> Iterable[Dict[str, Any]]:
    """
    Expected shape:
      {"earningsCalendar": [{"symbol":"AAPL","name":"Apple Inc","reportDate":"2025-10-06","reportTime":"amc", ...}, ...]}
    """
    if not isinstance(data, dict):
        return []
    # Handle AV throttle messages
    if any(k in data for k in ("Note", "Information", "Error Message")):
        msg = data.get("Note") or data.get("Information") or data.get("Error Message")
        raise AlphaVantageError(f"Alpha Vantage response: {msg}")
    arr = data.get("earningsCalendar") or []
    return arr if isinstance(arr, list) else []


def _parse_av_csv(text: str) -> Iterable[Dict[str, Any]]:
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    for row in reader:
        yield row


def _is_pre_market_report_time(value: str) -> bool:
    """
    Return True if Alpha Vantage's time-of-day field indicates
    a before-market-hours earnings release.

    Handles short codes ('bmo', 'bto') and text like
    'pre-market', 'before market open', etc.
    """
    if not value:
        return False

    v = value.strip().lower()

    # Short codes commonly used ("before market open")
    if v in {"bmo", "bto"}:
        return True

    # Text variants
    if "before" in v and "market" in v:
        return True
    if "pre-market" in v or "pre market" in v or "premarket" in v:
        return True

    return False


def _is_after_market_report_time(value: str) -> bool:
    """
    Return True if Alpha Vantage's time-of-day field indicates
    an after-market-hours earnings release.

    Handles short codes ('amc', 'pmc') and text like
    'after-market', 'after market close', 'after hours', 'post-market', etc.
    """
    if not value:
        return False

    v = value.strip().lower()

    # Short codes commonly used ("after market close")
    if v in {"amc", "pmc"}:
        return True

    # Text variants
    if "after" in v and "market" in v:
        return True
    if "after" in v and "close" in v:
        return True
    if "after-hours" in v or "after hours" in v or "afterhours" in v:
        return True
    if "post-market" in v or "post market" in v or "postmarket" in v:
        return True

    return False


async def fetch_alpha_vantage_calendar(
    session: aiohttp.ClientSession, *, horizon: str = "3month"
) -> List[Dict[str, Any]]:
    """
    Fetch AV earnings calendar (single request). Returns list[dict] of rows.
    Requires env var ALPHAVANTAGE_API_KEY.
    """
    if not ALPHAVANTAGE_API_KEY:
        raise AlphaVantageError("Set ALPHAVANTAGE_API_KEY in your environment.")

    params = {
        "function": ALPHAVANTAGE_FN,
        "horizon": horizon,
        "apikey": ALPHAVANTAGE_API_KEY,
        # "datatype": "json"
    }

    _log_http_request("Alpha Vantage", ALPHAVANTAGE_URL, params=params)
    async with session.get(ALPHAVANTAGE_URL, params=params, timeout=30) as resp:
        # AV often returns 200 even on throttle; detect by content
        text = await resp.text()
        rows: List[Dict[str, Any]] = []
        try:
            data = await resp.json()
            rows.extend(_parse_av_json(data))
            _log_http_response(
                "Alpha Vantage",
                ALPHAVANTAGE_URL,
                status=resp.status,
                params=params,
                details=f"json_rows={len(rows)}",
            )
        except Exception:
            # Not JSON (or had a Note/Error) → parse as CSV
            rows.extend(_parse_av_csv(text))
            _log_http_response(
                "Alpha Vantage",
                ALPHAVANTAGE_URL,
                status=resp.status,
                params=params,
                details=f"csv_rows={len(rows)}",
            )
        return rows


async def get_pre_market_next_session_and_after_market_today() -> Tuple[List[str], List[str]]:
    """
    Return two unique symbol lists from Alpha Vantage (US/Eastern):
      - pre_market_next_session:  next business day's pre-market earnings
      - after_market_today:   today's (or next business day's) after-market earnings

    If today is Sat/Sun, 'today' is taken as Monday (next business day),
    and 'next_session' is the next business day after that.
    """
    tz = ZoneInfo("America/New_York")
    today = datetime.now(tz).date()

    # Business "today": if weekend, roll forward to Monday
    days_ahead_today = 0
    while (today + timedelta(days=days_ahead_today)).weekday() >= 5:  # 5 = Sat, 6 = Sun
        days_ahead_today += 1
    business_today = today + timedelta(days=days_ahead_today)

    # Next business day after business_today
    days_ahead_next = 1
    while (business_today + timedelta(days=days_ahead_next)).weekday() >= 5:
        days_ahead_next += 1
    next_business_day = business_today + timedelta(days=days_ahead_next)
    logger.info(
        service_message("Alpha Vantage", "Fetching earnings calendar for business_today=%s next_business_day=%s."),
        business_today,
        next_business_day,
    )

    timeout = aiohttp.ClientTimeout(total=40)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        rows = await fetch_alpha_vantage_calendar(session)
    logger.info(service_message("Alpha Vantage", "Fetched %s earnings calendar rows."), len(rows))

    pre_market_next_session: List[str] = []
    after_market_today: List[str] = []
    seen_pre = set()
    seen_after = set()

    target_today = business_today.isoformat()
    target_tomorrow = next_business_day.isoformat()

    for r in rows:
        d = (r.get("reportDate") or "").strip()[:10]
        sym = (r.get("symbol") or "").strip().upper()
        if not sym:
            continue

        # Alpha Vantage CSV has 'timeOfTheDay' as the header for time-of-day info.
        # JSON (if ever used) may use 'reportTime'. Be robust and check multiple keys.
        rt_raw = (
            r.get("reportTime")
            or r.get("timeOfTheDay")
            or r.get("time_of_the_day")
            or r.get("time")
            or ""
        )
        rt_raw = rt_raw.strip()

        # After-market earnings for business_today
        if (
            d == target_today
            and sym not in seen_after
            and _is_after_market_report_time(rt_raw)
        ):
            seen_after.add(sym)
            after_market_today.append(sym)

        # Pre-market earnings for next_business_day
        if (
            d == target_tomorrow
            and sym not in seen_pre
            and _is_pre_market_report_time(rt_raw)
        ):
            seen_pre.add(sym)
            pre_market_next_session.append(sym)

    logger.info(
        service_message("Alpha Vantage", "Classification complete: pre_market_next_session=%s after_market_today=%s"),
        len(pre_market_next_session),
        len(after_market_today),
    )
    return pre_market_next_session, after_market_today


# -----------------------------
# Alpaca paper trading helpers
# -----------------------------
def get_alpaca_clients() -> Tuple[
    TradingClient,
]:
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        raise AlpacaConfigError(
            "Set ALPACA_API_KEY and ALPACA_SECRET_KEY before paper trading."
        )

    logger.info(service_message("Alpaca", "Initializing trading client for paper trading."))
    trade_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)
    return (trade_client,)


def get_account_value(trade_client: TradingClient) -> float:
    """
    Returns the account value field used for sizing, e.g. cash or buying_power.
    """
    logger.info(service_message("Alpaca", "Requesting account data for sizing using field '%s'."), ACCOUNT_VALUE_FIELD)
    account = call_with_retries(
        trade_client.get_account,
        service="Alpaca",
        action="get_account",
    )
    raw_value = getattr(account, ACCOUNT_VALUE_FIELD)
    value = float(raw_value)
    logger.info(service_message("Alpaca", "Fetched account field '%s' with value %.2f."), ACCOUNT_VALUE_FIELD, value)
    return value


def get_next_trading_session_date() -> date:
    return asyncio.run(_fetch_next_trading_session_date())


async def _fetch_tradier_spot_price(symbol: str) -> float:
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        return await get_current_price(session, symbol)


def get_spot_price(symbol: str) -> float:
    logger.info(service_symbol_message("Tradier", symbol, "Requesting latest stock quote."))
    price = asyncio.run(_fetch_tradier_spot_price(symbol))
    logger.info(service_symbol_message("Tradier", symbol, "Fetched latest stock quote: %.4f"), price)
    return price


async def _fetch_tradier_call_contracts(
    underlying: str,
    min_exp: date,
    max_exp: date,
    min_strike: float,
    max_strike: float,
) -> List[TradierOptionContract]:
    """
    Fetch all Tradier call contracts in the requested expiration / strike range.
    """
    logger.info(
        service_symbol_message("Tradier", underlying, "Fetching call contracts between expirations %s and %s with strike window %.2f-%.2f."),
        min_exp,
        max_exp,
        min_strike,
        max_strike,
    )
    all_contracts: List[TradierOptionContract] = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        expirations = await get_expirations(session, underlying)
        filtered_expirations = [
            exp for exp in expirations if min_exp <= datetime.strptime(exp, "%Y-%m-%d").date() <= max_exp
        ]
        logger.info(
            service_symbol_message("Tradier", underlying, "Expiration filtering kept %s of %s expirations."),
            len(filtered_expirations),
            len(expirations),
        )

        for expiration in filtered_expirations:
            calls, _ = await get_option_chain(session, underlying, expiration)
            if calls.empty:
                continue

            filtered_calls = calls[
                (calls["strike"] >= min_strike)
                & (calls["strike"] <= max_strike)
                & calls["symbol"].notna()
            ]
            if filtered_calls.empty:
                continue

            exp_date = datetime.strptime(expiration, "%Y-%m-%d").date()
            for row in filtered_calls.itertuples(index=False):
                all_contracts.append(
                    TradierOptionContract(
                        symbol=str(row.symbol),
                        expiration_date=exp_date,
                        strike_price=float(row.strike),
                        bid=float(row.bid),
                        ask=float(row.ask),
                    )
                )

    logger.info(service_symbol_message("Tradier", underlying, "Collected %s call contracts."), len(all_contracts))
    return all_contracts


def fetch_call_contracts(
    underlying: str,
    min_exp: date,
    max_exp: date,
    min_strike: float,
    max_strike: float,
) -> List[TradierOptionContract]:
    return asyncio.run(
        _fetch_tradier_call_contracts(
            underlying=underlying,
            min_exp=min_exp,
            max_exp=max_exp,
            min_strike=min_strike,
            max_strike=max_strike,
        )
    )


def choose_expirations(
    contracts: List[Any],
    short_target: date,
    long_target: date,
) -> Tuple[date, date]:
    """
    Choose:
      - short expiration = nearest available expiration on/after short_target
      - long expiration  = nearest later expiration to long_target
    """
    expirations = sorted({contract.expiration_date for contract in contracts})

    short_candidates = [exp for exp in expirations if exp >= short_target]
    if not short_candidates:
        raise ValueError("No available expiration on/after the next trading session.")

    short_exp = min(short_candidates, key=lambda exp: (abs((exp - short_target).days), exp))

    long_candidates = [exp for exp in expirations if exp > short_exp]
    if not long_candidates:
        raise ValueError("No longer-dated expiration after the short leg expiration.")

    long_exp = min(long_candidates, key=lambda exp: (abs((exp - long_target).days), exp))

    return short_exp, long_exp


def build_candidate_spread(
    underlying: str,
) -> SpreadCandidate:
    """
    Builds a long call calendar spread using only the ATM strike.
    ATM is defined as the common strike between both expirations that is
    closest to the current underlying spot price.
    """
    next_session = get_next_trading_session_date()
    long_target = next_session + timedelta(days=30)
    logger.info(
        symbol_message(underlying, "Building candidate spread with next_session=%s long_target=%s."),
        next_session,
        long_target,
    )

    spot = get_spot_price(underlying)

    min_strike = max(0.01, spot * (1.0 - STRIKE_WINDOW_PCT))
    max_strike = spot * (1.0 + STRIKE_WINDOW_PCT)

    contracts = fetch_call_contracts(
        underlying=underlying,
        min_exp=next_session,
        max_exp=long_target + timedelta(days=21),
        min_strike=min_strike,
        max_strike=max_strike,
    )

    if not contracts:
        raise ValueError("No call contracts returned in the target expiration/strike window.")

    short_exp, long_exp = choose_expirations(contracts, next_session, long_target)
    logger.info(symbol_message(underlying, "Selected expirations short=%s long=%s."), short_exp, long_exp)

    short_by_strike: Dict[float, Any] = {
        float(contract.strike_price): contract
        for contract in contracts
        if contract.expiration_date == short_exp
    }
    long_by_strike: Dict[float, Any] = {
        float(contract.strike_price): contract
        for contract in contracts
        if contract.expiration_date == long_exp
    }

    common_strikes = list(set(short_by_strike.keys()) & set(long_by_strike.keys()))
    if not common_strikes:
        raise ValueError("No common strike exists between the chosen expirations.")

    atm_strike = min(common_strikes, key=lambda strike: abs(strike - spot))
    logger.info(symbol_message(underlying, "Selected ATM strike %.2f from %s common strikes."), atm_strike, len(common_strikes))

    short_contract = short_by_strike[atm_strike]
    long_contract = long_by_strike[atm_strike]

    short_bid = float(short_contract.bid or 0.0)
    short_ask = float(short_contract.ask or 0.0)
    long_bid = float(long_contract.bid or 0.0)
    long_ask = float(long_contract.ask or 0.0)

    if long_ask <= 0:
        raise ValueError(f"Invalid long ask for ATM strike {atm_strike}")
    if short_bid < 0:
        raise ValueError(f"Invalid short bid for ATM strike {atm_strike}")

    natural_debit = long_ask - short_bid
    mid_debit = ((long_bid + long_ask) / 2.0) - ((short_bid + short_ask) / 2.0)

    if natural_debit <= 0:
        raise ValueError(f"Non-positive natural debit for ATM strike {atm_strike}")

    logger.info(
        symbol_message(underlying, "Candidate spread ready: spot=%.2f strike=%.2f short=%s long=%s natural_debit=%.2f mid_debit=%.2f"),
        spot,
        atm_strike,
        short_contract.symbol,
        long_contract.symbol,
        natural_debit,
        mid_debit,
    )
    return SpreadCandidate(
        underlying=underlying,
        spot=spot,
        strike=atm_strike,
        short_exp=short_exp,
        long_exp=long_exp,
        short_symbol=short_contract.symbol,
        long_symbol=long_contract.symbol,
        short_bid=short_bid,
        short_ask=short_ask,
        long_bid=long_bid,
        long_ask=long_ask,
        natural_debit=natural_debit,
        mid_debit=mid_debit,
    )


def choose_entry_debit(candidate: SpreadCandidate) -> float:
    base_debit = candidate.mid_debit if USE_MID_DEBIT else candidate.natural_debit
    buffered_debit = base_debit * (1.0 + MARKET_ORDER_SLIPPAGE_PCT)
    debit = max(buffered_debit, MIN_NET_DEBIT)
    return round(debit, 2)


def choose_qty(
    debit_per_spread: float,
    target_budget_dollars: float,
    available_value_dollars: float,
) -> int:
    """
    Chooses the integer number of spreads whose total cost is closest to the
    target budget, without exceeding available funds.
    """
    per_spread_cost = debit_per_spread * 100.0
    if per_spread_cost <= 0:
        return 0
    if per_spread_cost > available_value_dollars:
        return 0

    raw_qty = target_budget_dollars / per_spread_cost

    candidates = {
        max(1, math.floor(raw_qty)),
        max(1, math.ceil(raw_qty)),
        1,
    }

    feasible = [
        qty for qty in candidates
        if qty >= 1 and (qty * per_spread_cost) <= available_value_dollars
    ]
    if not feasible:
        return 0

    return min(
        feasible,
        key=lambda qty: (abs((qty * per_spread_cost) - target_budget_dollars), -qty)
    )


def make_order_request(
    candidate: SpreadCandidate,
    qty: int,
    entry_debit: float,
) -> MarketOrderRequest:
    del entry_debit
    return MarketOrderRequest(
        qty=qty,
        type=OrderType.MARKET,
        time_in_force=TimeInForce.DAY,
        order_class=OrderClass.MLEG,
        client_order_id=(
            f"cal-{candidate.underlying.lower()}-"
            f"{candidate.short_exp:%Y%m%d}-{candidate.long_exp:%Y%m%d}-"
            f"{int(candidate.strike * 100):08d}"
        ),
        legs=[
            OptionLegRequest(
                symbol=candidate.short_symbol,
                ratio_qty=1,
                side=OrderSide.SELL,
                position_intent="sell_to_open",
            ),
            OptionLegRequest(
                symbol=candidate.long_symbol,
                ratio_qty=1,
                side=OrderSide.BUY,
                position_intent="buy_to_open",
            ),
        ],
    )


def paper_trade_calendar_spreads(tickers: List[str]) -> None:
    tickers = [ticker.upper().strip() for ticker in tickers if ticker.strip()]
    if not tickers:
        logger.info(service_message("Workflow", "No ticker symbols supplied for paper trading."))
        return

    (trade_client,) = get_alpaca_clients()

    initial_available = get_account_value(trade_client)
    shared_total_budget = initial_available * PCT_OF_AVAILABLE
    remaining_shared_budget = shared_total_budget

    logger.info(
        service_message("Workflow", "Starting paper trade session: tickers=%s account_value_field=%s initial_available=%.2f budget_mode=%s dry_run=%s"),
        tickers,
        ACCOUNT_VALUE_FIELD,
        initial_available,
        BUDGET_MODE,
        DRY_RUN,
    )

    for i, ticker in enumerate(tickers):
        try:
            current_available = get_account_value(trade_client)

            if BUDGET_MODE == "shared_total":
                remaining_names = len(tickers) - i
                target_budget = max(0.0, remaining_shared_budget / max(1, remaining_names))
            else:
                target_budget = current_available * PCT_OF_AVAILABLE
            logger.info(
                symbol_message(ticker, "Budget evaluation: current_available=%.2f target_budget=%.2f remaining_shared_budget=%.2f"),
                current_available,
                target_budget,
                remaining_shared_budget,
            )

            candidate = build_candidate_spread(
                underlying=ticker,
            )
            entry_debit = choose_entry_debit(candidate)
            base_entry_debit = candidate.mid_debit if USE_MID_DEBIT else candidate.natural_debit
            qty = choose_qty(
                debit_per_spread=entry_debit,
                target_budget_dollars=target_budget,
                available_value_dollars=current_available,
            )

            if qty < 1:
                logger.info(symbol_message(ticker, "Skipped: no feasible quantity at estimated entry debit %.2f."), entry_debit)
                continue

            est_total_cost = qty * entry_debit * 100.0
            req = make_order_request(candidate, qty, entry_debit)

            logger.info(
                symbol_message(ticker, "Order candidate: spot=%.2f strike=%.2f short_exp=%s short_symbol=%s long_exp=%s long_symbol=%s short_quote=%.2f/%.2f long_quote=%.2f/%.2f base_entry_debit=%.2f slippage_pct=%.2f estimated_entry_debit=%.2f target_budget=%.2f qty=%s est_total_debit=%.2f"),
                candidate.spot,
                candidate.strike,
                candidate.short_exp,
                candidate.short_symbol,
                candidate.long_exp,
                candidate.long_symbol,
                candidate.short_bid,
                candidate.short_ask,
                candidate.long_bid,
                candidate.long_ask,
                base_entry_debit,
                MARKET_ORDER_SLIPPAGE_PCT,
                entry_debit,
                target_budget,
                qty,
                est_total_cost,
            )

            if DRY_RUN:
                logger.info(symbol_message(ticker, "DRY RUN enabled; order not submitted."))
            else:
                logger.info(
                    service_symbol_message("Alpaca", ticker, "Submitting market calendar spread order with client_order_id=%s."),
                    req.client_order_id,
                )
                order = call_with_retries(
                    lambda: trade_client.submit_order(req),
                    service="Alpaca",
                    action="submit_order",
                )
                logger.info(
                    symbol_message(ticker, "Order submitted successfully: order_id=%s status=%s"),
                    order.id,
                    order.status,
                )

            if BUDGET_MODE == "shared_total":
                remaining_shared_budget -= est_total_cost
                logger.info(symbol_message(ticker, "Updated remaining shared budget to %.2f."), remaining_shared_budget)

        except Exception as exc:
            logger.error(symbol_message(ticker, "Skipped due to error: %s"), exc)


# -----------------------------
# End-to-end (async)
# -----------------------------
async def async_main() -> List[str]:
    results_pre: Dict[str, str] = {}
    results_after: Dict[str, str] = {}

    logger.info(service_message("Workflow", "Starting async earnings discovery workflow."))
    try:
        pre_market_syms, after_market_syms = await get_pre_market_next_session_and_after_market_today()
    except AlphaVantageError as e:
        logger.error(service_message("Alpha Vantage", "Error while fetching earnings calendar: %s"), e)
        return []
    except Exception as e:
        logger.error(service_message("Alpha Vantage", "Unexpected error while fetching earnings calendar: %s"), e)
        return []

    logger.info(service_message("Workflow", "Pre-market next session candidate count: %s"), len(pre_market_syms))
    logger.info(service_message("Workflow", "After-market today candidate count: %s"), len(after_market_syms))

    if not pre_market_syms and not after_market_syms:
        logger.info(service_message("Workflow", "No relevant earnings events found; nothing to score."))
        return []

    try:
        sp500 = get_sp500_tickers()
    except Exception as exc:
        logger.error(service_message("DataHub", "Failed to load S&P 500 membership before recommendation scoring: %s"), exc)
        return []

    pre_market_syms = filter_symbols_to_sp500(pre_market_syms, sp500)
    after_market_syms = filter_symbols_to_sp500(after_market_syms, sp500)
    logger.info(
        service_message("Workflow", "Post-S&P 500 filter candidate count: pre_market_next_session=%s after_market_today=%s"),
        len(pre_market_syms),
        len(after_market_syms),
    )

    if not pre_market_syms and not after_market_syms:
        logger.info(service_message("Workflow", "No S&P 500 earnings candidates remained after filtering; nothing to score."))
        return []

    timeout = aiohttp.ClientTimeout(total=30)
    sem_tradier = asyncio.Semaphore(6)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # --- Pre-market tomorrow ---
        if pre_market_syms:
            logger.info(service_message("Workflow", "Scoring pre-market list for next session: %s symbols."), len(pre_market_syms))

            async def eval_one_pre(t: str) -> Optional[Tuple[str, str]]:
                async with sem_tradier:
                    rec = await recommend_ticker(session, t)
                    return (t, rec) if rec != "Avoid" else None

            tasks_pre = [asyncio.create_task(eval_one_pre(t)) for t in pre_market_syms]
            for fut in asyncio.as_completed(tasks_pre):
                r = await fut
                if r:
                    k, v = r
                    results_pre[k] = v

        # --- After-market today ---
        if after_market_syms:
            logger.info(service_message("Workflow", "Scoring after-market list for today: %s symbols."), len(after_market_syms))

            async def eval_one_after(t: str) -> Optional[Tuple[str, str]]:
                async with sem_tradier:
                    rec = await recommend_ticker(session, t)
                    return (t, rec) if rec != "Avoid" else None

            tasks_after = [asyncio.create_task(eval_one_after(t)) for t in after_market_syms]
            for fut in asyncio.as_completed(tasks_after):
                r = await fut
                if r:
                    k, v = r
                    results_after[k] = v

    recommended_pre = sorted(sym for sym, rec in results_pre.items() if rec == "Recommended")
    recommended_after = sorted(sym for sym, rec in results_after.items() if rec == "Recommended")
    matching_symbols = sorted(set(recommended_pre) | set(recommended_after))

    logger.info(service_message("Workflow", "Finished recommendation scoring. Matching ticker symbols follow."))
    if matching_symbols:
        logger.info(service_message("Workflow", "Matching ticker symbols: %s"), matching_symbols)
    else:
        logger.info(
            service_message("Workflow", "No ticker symbols met all criteria: after-market today or pre-market next session, S&P 500, and Recommended.")
        )

    return matching_symbols


def run_trading_session() -> Dict[str, Any]:
    configure_logging()
    logger.info(service_message("Workflow", "Starting end-to-end trading session run."))
    symbols = asyncio.run(async_main())
    if symbols:
        logger.info(service_message("Workflow", "Proceeding to paper-trade %s matching symbols."), len(symbols))
        paper_trade_calendar_spreads(symbols)
    else:
        logger.info(service_message("Workflow", "No matching symbols found; skipping paper trading."))
    return {
        "matching_symbols": symbols,
        "submitted_symbol_count": len(symbols),
    }


if __name__ == "__main__":
    configure_logging()
    try:
        run_trading_session()
    except AlpacaConfigError as exc:
        logger.error(service_message("Alpaca", "Paper trading skipped due to configuration error: %s"), exc)
