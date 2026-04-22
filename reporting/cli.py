from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import shlex
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Optional

import click
import requests
import typer
from rich import box
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="Alpaca paper-trading 2-leg calendar spread history CLI")
console = Console()
STATE = {"db_path": str(Path("spreadhist.db").resolve())}

PAPER_BASE_URL = "https://paper-api.alpaca.markets"
DATA_BASE_URL = "https://data.alpaca.markets"
OPTION_ACTIVITY_TYPES = {"FILL", "OPASN", "OPEXP", "OPXRC", "OPEXC", "OPTRD"}
ORDER_FINAL_STATUSES = {"filled", "partially_filled"}
ORDER_CLOSE_INTENTS = {"buy_to_close", "sell_to_close"}
ORDER_OPEN_INTENTS = {"buy_to_open", "sell_to_open"}
OPTION_SYMBOL_RE = re.compile(r"^(?P<underlying>[A-Z.]+)(?P<yymmdd>\d{6})(?P<cp>[CP])(?P<strike>\d{8})$")
PORTFOLIO_INITIAL_VALUE = Decimal("100000")


@dataclass(slots=True)
class OptionContract:
    underlying: str
    expiration: date
    option_type: str
    strike: Decimal


@dataclass(slots=True)
class OrderLegEvent:
    order_id: str
    parent_order_id: Optional[str]
    client_order_id: Optional[str]
    symbol: str
    side: str
    position_intent: Optional[str]
    qty: int
    filled_qty: int
    price: Decimal
    filled_at: datetime
    submitted_at: datetime
    status: str
    order_class: str
    contract: OptionContract
    raw: dict[str, Any]

    @property
    def timestamp(self) -> datetime:
        return self.filled_at or self.submitted_at


@dataclass(slots=True)
class GroupedSpreadEvent:
    event_id: str
    parent_order_id: str
    timestamp: datetime
    qty: int
    underlying: str
    option_type: str
    strike: Decimal
    short_symbol: str
    short_expiration: date
    short_price: Decimal
    long_symbol: str
    long_expiration: date
    long_price: Decimal
    intent: str  # entry or exit
    source: str = "parent"
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SpreadState:
    spread_id: str
    source: str
    group_mode: str
    underlying: str
    option_type: str
    strike: Decimal
    qty: int
    opened_at: datetime
    short_symbol: str
    short_expiration: date
    long_symbol: str
    long_expiration: date
    entry_debit: Decimal
    cash_flow: Decimal = Decimal("0")
    close_value: Decimal = Decimal("0")
    closed_at: Optional[datetime] = None
    status: str = "open"
    opening_order_id: Optional[str] = None
    closing_order_ids: list[str] = field(default_factory=list)
    short_closed_qty: int = 0
    long_closed_qty: int = 0
    short_resolved: Optional[str] = None
    long_resolved: Optional[str] = None
    notes: list[str] = field(default_factory=list)
    order_events: list[dict[str, Any]] = field(default_factory=list)
    assignment_or_exercise: bool = False
    incomplete_economics: bool = False

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.underlying, self.option_type, str(self.strike), str(self.qty))

    def add_note(self, note: str) -> None:
        if note not in self.notes:
            self.notes.append(note)

    @property
    def realized_pnl(self) -> Decimal:
        return self.cash_flow

    @property
    def realized_pnl_percent(self) -> Optional[Decimal]:
        return percent_change(self.realized_pnl, abs(self.entry_debit))

    @property
    def avg_open_fill_price(self) -> Decimal:
        if self.qty <= 0:
            return Decimal("0")
        return self.entry_debit / (Decimal(100) * Decimal(self.qty))

    @property
    def total_open_price(self) -> Decimal:
        return self.entry_debit

    @property
    def has_close_pricing(self) -> bool:
        return bool(
            self.closed_at
            or self.closing_order_ids
            or self.short_resolved in {"expired", "assigned", "exercised", "closed"}
            or self.long_resolved in {"expired", "assigned", "exercised", "closed"}
        )

    @property
    def avg_close_fill_price(self) -> Optional[Decimal]:
        if not self.has_close_pricing:
            return None
        if self.qty <= 0:
            return Decimal("0")
        return self.close_value / (Decimal(100) * Decimal(self.qty))

    @property
    def total_close_price(self) -> Optional[Decimal]:
        if not self.has_close_pricing:
            return None
        return self.close_value

    def current_value(self, positions_by_symbol: dict[str, dict[str, Any]]) -> Decimal:
        total = Decimal("0")
        for symbol in (self.short_symbol, self.long_symbol):
            pos = positions_by_symbol.get(symbol)
            if not pos:
                continue
            total += parse_decimal(pos.get("market_value"))
        return total

    def mark_status_from_positions(self, positions_by_symbol: dict[str, dict[str, Any]]) -> None:
        short_open = self.short_symbol in positions_by_symbol
        long_open = self.long_symbol in positions_by_symbol
        if short_open and long_open:
            if self.status == "open":
                return
        if not short_open and not long_open and self.closed_at:
            self.status = "closed"
            return
        if not short_open and not long_open and not self.closed_at:
            self.status = "resolved_unmatched"
            self.add_note("No open leg remains, but no full close order pair was found.")
            return
        self.status = "broken"
        if short_open and not long_open:
            self.add_note("Only the short leg remains open.")
        elif long_open and not short_open:
            self.add_note("Only the long leg remains open.")


@dataclass(slots=True)
class LegPerformance:
    label: str
    symbol: str
    status: str
    open_cash: Decimal
    close_cash: Decimal
    current_value: Decimal

    @property
    def pnl(self) -> Decimal:
        return self.open_cash + self.close_cash + self.current_value

    @property
    def percent_change(self) -> Optional[Decimal]:
        return percent_change(self.pnl, abs(self.open_cash))


def parse_decimal(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(default)


def parse_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return default


def parse_iso_datetime(value: Optional[str]) -> datetime:
    if not value:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def parse_portfolio_timestamp(value: Any) -> datetime:
    raw = str(value or "")
    try:
        return datetime.fromtimestamp(int(raw), tz=timezone.utc)
    except (OSError, ValueError):
        parsed = parse_iso_datetime(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


DATE_WINDOW_TOKEN_RE = re.compile(r"^(?P<amount>\d+)(?P<unit>[dwmyDWMY])$")


@dataclass(slots=True)
class DateWindow:
    start: Optional[datetime]
    end_exclusive: Optional[datetime]
    label: str = "all time"

    @property
    def is_all_time(self) -> bool:
        return self.start is None and self.end_exclusive is None


def format_window_bound(value: Optional[datetime]) -> str:
    if value is None:
        return "unbounded"
    return value.astimezone(timezone.utc).isoformat()


def print_final_timeframe(window: DateWindow, *, basis: str) -> None:
    console.print(
        "Final timeframe: "
        f"{window.label} | "
        f"start={format_window_bound(window.start)} | "
        f"end_exclusive={format_window_bound(window.end_exclusive)} | "
        f"basis={basis}"
    )


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def shift_datetime_months(value: datetime, months: int) -> datetime:
    month_index = (value.month - 1) + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1

    if month == 12:
        next_month = datetime(year + 1, 1, 1, tzinfo=value.tzinfo)
    else:
        next_month = datetime(year, month + 1, 1, tzinfo=value.tzinfo)
    last_day = (next_month - timedelta(days=1)).day
    day = min(value.day, last_day)
    return value.replace(year=year, month=month, day=day)


def parse_user_datetime(value: str, *, as_date_end_exclusive: bool = False) -> datetime:
    raw = value.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        parsed_date = parse_date(raw)
        if as_date_end_exclusive:
            parsed_date += timedelta(days=1)
        return datetime(parsed_date.year, parsed_date.month, parsed_date.day, tzinfo=timezone.utc)

    parsed_dt = parse_iso_datetime(raw)
    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
    return parsed_dt.astimezone(timezone.utc)


def parse_since_value(value: str, *, now: Optional[datetime] = None) -> datetime:
    now = now or utc_now()
    raw = value.strip()
    token_match = DATE_WINDOW_TOKEN_RE.fullmatch(raw)
    if token_match:
        amount = int(token_match.group("amount"))
        unit = token_match.group("unit").lower()
        if unit == "d":
            return now - timedelta(days=amount)
        if unit == "w":
            return now - timedelta(weeks=amount)
        if unit == "m":
            return shift_datetime_months(now, -amount)
        if unit == "y":
            return shift_datetime_months(now, -(amount * 12))
    return parse_user_datetime(raw)


def resolve_date_window(
    *,
    since: Optional[str],
    until: Optional[str],
    ytd: bool,
    all_time: bool,
    now: Optional[datetime] = None,
) -> DateWindow:
    now = now or utc_now()
    if since and ytd:
        raise typer.BadParameter("Use either --since or --ytd, not both.")
    if all_time and (since or until or ytd):
        raise typer.BadParameter("Use --all by itself, without --since, --until, or --ytd.")

    start: Optional[datetime] = None
    end_exclusive: Optional[datetime] = None
    label_parts: list[str] = []

    if since:
        start = parse_since_value(since, now=now)
        label_parts.append(f"since {since}")
    elif ytd:
        start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
        label_parts.append("year-to-date")
    elif all_time:
        label_parts.append("all time")

    if until:
        end_exclusive = parse_user_datetime(until, as_date_end_exclusive=True)
        label_parts.append(f"until {until}")

    if start and end_exclusive and end_exclusive <= start:
        raise typer.BadParameter("--until must be after --since / --ytd.")

    if not label_parts:
        label_parts.append("all time")

    return DateWindow(start=start, end_exclusive=end_exclusive, label=", ".join(label_parts))


def datetime_in_window(value: datetime, window: DateWindow) -> bool:
    if window.start and value < window.start:
        return False
    if window.end_exclusive and value >= window.end_exclusive:
        return False
    return True


def spread_in_window(spread: SpreadState, window: DateWindow, *, mode: str = "anchor") -> bool:
    if window.is_all_time:
        return True

    if mode == "opened":
        return datetime_in_window(spread.opened_at, window)

    if mode == "closed":
        return spread.closed_at is not None and datetime_in_window(spread.closed_at, window)

    if mode == "anchor":
        anchor = spread.closed_at or spread.opened_at
        return datetime_in_window(anchor, window)

    if mode == "overlap":
        spread_end = spread.closed_at or datetime.max.replace(tzinfo=timezone.utc)
        if window.start and spread_end < window.start:
            return False
        if window.end_exclusive and spread.opened_at >= window.end_exclusive:
            return False
        return True

    raise ValueError(f"Unsupported window mode: {mode}")


def filter_spreads_by_window(spreads: Iterable[SpreadState], window: DateWindow, *, mode: str = "anchor") -> list[SpreadState]:
    return [spread for spread in spreads if spread_in_window(spread, window, mode=mode)]


def parse_option_symbol(symbol: str) -> Optional[OptionContract]:
    match = OPTION_SYMBOL_RE.match(symbol or "")
    if not match:
        return None
    expiration = datetime.strptime(match.group("yymmdd"), "%y%m%d").date()
    strike = Decimal(match.group("strike")) / Decimal("1000")
    return OptionContract(
        underlying=match.group("underlying"),
        expiration=expiration,
        option_type="call" if match.group("cp") == "C" else "put",
        strike=strike,
    )


class AlpacaClient:
    def __init__(self, api_key: str, api_secret: str, paper: bool = True):
        self.base_url = PAPER_BASE_URL if paper else "https://api.alpaca.markets"
        self.data_url = DATA_BASE_URL
        self.session = requests.Session()
        self.session.headers.update(
            {
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": api_secret,
                "Accept": "application/json",
            }
        )

    def get(self, path: str, *, params: Optional[dict[str, Any]] = None, data_api: bool = False) -> Any:
        base = self.data_url if data_api else self.base_url
        url = f"{base}{path}"
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def fetch_orders(self, start: Optional[str] = None, end: Optional[str] = None) -> list[dict[str, Any]]:
        orders: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        page_until = end
        while True:
            params: dict[str, Any] = {
                "status": "all",
                "nested": "true",
                "direction": "desc",
                "limit": 500,
            }
            if start:
                params["after"] = start
            if page_until:
                params["until"] = page_until
            batch = self.get("/v2/orders", params=params)
            if not isinstance(batch, list) or not batch:
                break
            for item in batch:
                order_id = item.get("id")
                if order_id in seen_ids:
                    continue
                seen_ids.add(order_id)
                orders.append(item)
            if len(batch) < 500:
                break
            oldest = min((o.get("submitted_at") for o in batch if o.get("submitted_at")), default=None)
            if not oldest:
                break
            next_until = (parse_iso_datetime(oldest) - timedelta(microseconds=1)).isoformat()
            if page_until == next_until:
                break
            page_until = next_until
        return orders

    def fetch_account_activities(self, start: Optional[str] = None, end: Optional[str] = None) -> list[dict[str, Any]]:
        activities: list[dict[str, Any]] = []
        page_token: Optional[str] = None
        while True:
            params: dict[str, Any] = {"direction": "desc", "page_size": 100}
            if start:
                params["after"] = start
            if end:
                params["until"] = end
            if page_token:
                params["page_token"] = page_token
            batch = self.get("/v2/account/activities", params=params)
            if not isinstance(batch, list) or not batch:
                break
            activities.extend(batch)
            if len(batch) < 100:
                break
            last_id = batch[-1].get("id")
            if not last_id or page_token == last_id:
                break
            page_token = last_id
        return activities

    def fetch_positions(self) -> list[dict[str, Any]]:
        positions = self.get("/v2/positions")
        return positions if isinstance(positions, list) else []

    def fetch_portfolio_history(self, start: Optional[str] = None, end: Optional[str] = None) -> dict[str, Any]:
        params: dict[str, Any] = {"timeframe": "1D"}
        if start or end:
            if start:
                params["start"] = start
            if end:
                params["end"] = end
            if not start:
                params["period"] = "1A"
        else:
            params["period"] = "1A"
        return self.get("/v2/account/portfolio/history", params=params)


class Database:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.ensure_schema()

    def ensure_schema(self) -> None:
        self.conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS raw_orders (
                order_id TEXT PRIMARY KEY,
                parent_order_id TEXT,
                client_order_id TEXT,
                symbol TEXT,
                side TEXT,
                position_intent TEXT,
                qty INTEGER,
                filled_qty INTEGER,
                filled_avg_price TEXT,
                status TEXT,
                order_class TEXT,
                type TEXT,
                time_in_force TEXT,
                limit_price TEXT,
                submitted_at TEXT,
                filled_at TEXT,
                canceled_at TEXT,
                expired_at TEXT,
                failed_at TEXT,
                asset_class TEXT,
                raw_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS raw_activities (
                activity_id TEXT PRIMARY KEY,
                activity_type TEXT,
                transaction_time TEXT,
                activity_date TEXT,
                symbol TEXT,
                qty TEXT,
                side TEXT,
                price TEXT,
                net_amount TEXT,
                status TEXT,
                raw_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS raw_positions (
                symbol TEXT PRIMARY KEY,
                qty TEXT,
                side TEXT,
                avg_entry_price TEXT,
                cost_basis TEXT,
                market_value TEXT,
                unrealized_pl TEXT,
                current_price TEXT,
                raw_json TEXT NOT NULL,
                synced_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS raw_portfolio_history (
                ts TEXT PRIMARY KEY,
                equity TEXT,
                profit_loss TEXT,
                profit_loss_pct TEXT,
                base_value TEXT,
                raw_json TEXT NOT NULL
            );
            """
        )
        self.conn.commit()

    def upsert_metadata(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO metadata(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def upsert_orders(self, orders: Iterable[dict[str, Any]]) -> int:
        count = 0
        for order in orders:
            self.conn.execute(
                """
                INSERT INTO raw_orders(
                    order_id, parent_order_id, client_order_id, symbol, side, position_intent,
                    qty, filled_qty, filled_avg_price, status, order_class, type, time_in_force,
                    limit_price, submitted_at, filled_at, canceled_at, expired_at, failed_at,
                    asset_class, raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(order_id) DO UPDATE SET
                    parent_order_id=excluded.parent_order_id,
                    client_order_id=excluded.client_order_id,
                    symbol=excluded.symbol,
                    side=excluded.side,
                    position_intent=excluded.position_intent,
                    qty=excluded.qty,
                    filled_qty=excluded.filled_qty,
                    filled_avg_price=excluded.filled_avg_price,
                    status=excluded.status,
                    order_class=excluded.order_class,
                    type=excluded.type,
                    time_in_force=excluded.time_in_force,
                    limit_price=excluded.limit_price,
                    submitted_at=excluded.submitted_at,
                    filled_at=excluded.filled_at,
                    canceled_at=excluded.canceled_at,
                    expired_at=excluded.expired_at,
                    failed_at=excluded.failed_at,
                    asset_class=excluded.asset_class,
                    raw_json=excluded.raw_json
                """,
                (
                    order.get("id"),
                    order.get("parent_order_id"),
                    order.get("client_order_id"),
                    order.get("symbol"),
                    order.get("side"),
                    order.get("position_intent"),
                    parse_int(order.get("qty")),
                    parse_int(order.get("filled_qty")),
                    str(order.get("filled_avg_price") or ""),
                    order.get("status"),
                    order.get("order_class"),
                    order.get("type"),
                    order.get("time_in_force"),
                    str(order.get("limit_price") or ""),
                    order.get("submitted_at"),
                    order.get("filled_at"),
                    order.get("canceled_at"),
                    order.get("expired_at"),
                    order.get("failed_at"),
                    order.get("asset_class"),
                    json.dumps(order, sort_keys=True),
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def replace_positions(self, positions: Iterable[dict[str, Any]]) -> int:
        synced_at = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute("DELETE FROM raw_positions")
        count = 0
        for pos in positions:
            self.conn.execute(
                """
                INSERT INTO raw_positions(
                    symbol, qty, side, avg_entry_price, cost_basis,
                    market_value, unrealized_pl, current_price, raw_json, synced_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    pos.get("symbol"),
                    str(pos.get("qty") or ""),
                    pos.get("side"),
                    str(pos.get("avg_entry_price") or ""),
                    str(pos.get("cost_basis") or ""),
                    str(pos.get("market_value") or ""),
                    str(pos.get("unrealized_pl") or ""),
                    str(pos.get("current_price") or ""),
                    json.dumps(pos, sort_keys=True),
                    synced_at,
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def upsert_activities(self, activities: Iterable[dict[str, Any]]) -> int:
        count = 0
        for activity in activities:
            self.conn.execute(
                """
                INSERT INTO raw_activities(
                    activity_id, activity_type, transaction_time, activity_date,
                    symbol, qty, side, price, net_amount, status, raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(activity_id) DO UPDATE SET
                    activity_type=excluded.activity_type,
                    transaction_time=excluded.transaction_time,
                    activity_date=excluded.activity_date,
                    symbol=excluded.symbol,
                    qty=excluded.qty,
                    side=excluded.side,
                    price=excluded.price,
                    net_amount=excluded.net_amount,
                    status=excluded.status,
                    raw_json=excluded.raw_json
                """,
                (
                    activity.get("id"),
                    activity.get("activity_type"),
                    activity.get("transaction_time") or activity.get("date"),
                    activity.get("date"),
                    activity.get("symbol"),
                    str(activity.get("qty") or ""),
                    activity.get("side"),
                    str(activity.get("price") or ""),
                    str(activity.get("net_amount") or ""),
                    activity.get("status"),
                    json.dumps(activity, sort_keys=True),
                ),
            )
            count += 1
        self.conn.commit()
        return count

    @staticmethod
    def _normalize_history_series(value: Any, length: int) -> list[Any]:
        if length <= 0:
            return []
        if value is None:
            return [None] * length
        if isinstance(value, (list, tuple)):
            series = list(value)
        else:
            series = [value] * length
        if len(series) < length:
            series.extend([None] * (length - len(series)))
        return series[:length]

    def replace_portfolio_history(self, payload: dict[str, Any]) -> int:
        raw_timestamps = payload.get("timestamp")
        if raw_timestamps is None:
            timestamps: list[Any] = []
        elif isinstance(raw_timestamps, (list, tuple)):
            timestamps = list(raw_timestamps)
        else:
            timestamps = [raw_timestamps]

        row_count = len(timestamps)
        if row_count == 0:
            self.conn.commit()
            return 0

        equity = self._normalize_history_series(payload.get("equity"), row_count)
        profit_loss = self._normalize_history_series(payload.get("profit_loss"), row_count)
        profit_loss_pct = self._normalize_history_series(payload.get("profit_loss_pct"), row_count)
        base_value = self._normalize_history_series(payload.get("base_value"), row_count)
        count = 0
        for idx, ts in enumerate(timestamps):
            record = {
                "ts": ts,
                "equity": equity[idx],
                "profit_loss": profit_loss[idx],
                "profit_loss_pct": profit_loss_pct[idx],
                "base_value": base_value[idx],
            }
            self.conn.execute(
                """
                INSERT INTO raw_portfolio_history(ts, equity, profit_loss, profit_loss_pct, base_value, raw_json)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(ts) DO UPDATE SET
                    equity=excluded.equity,
                    profit_loss=excluded.profit_loss,
                    profit_loss_pct=excluded.profit_loss_pct,
                    base_value=excluded.base_value,
                    raw_json=excluded.raw_json
                """,
                (
                    str(record["ts"]),
                    str(record["equity"] or ""),
                    str(record["profit_loss"] or ""),
                    str(record["profit_loss_pct"] or ""),
                    str(record["base_value"] or ""),
                    json.dumps(record, sort_keys=True),
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def fetch_raw_orders(self) -> list[dict[str, Any]]:
        rows = self.conn.execute("SELECT raw_json FROM raw_orders").fetchall()
        return [json.loads(row[0]) for row in rows]

    def fetch_raw_activities(self) -> list[dict[str, Any]]:
        rows = self.conn.execute("SELECT raw_json FROM raw_activities").fetchall()
        return [json.loads(row[0]) for row in rows]

    def fetch_raw_positions(self) -> list[dict[str, Any]]:
        rows = self.conn.execute("SELECT raw_json FROM raw_positions").fetchall()
        return [json.loads(row[0]) for row in rows]

    def fetch_portfolio_history(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT ts, equity, profit_loss, profit_loss_pct, base_value
            FROM raw_portfolio_history
            ORDER BY ts
            """
        ).fetchall()
        return [
            {
                "ts": row[0],
                "equity": row[1],
                "profit_loss": row[2],
                "profit_loss_pct": row[3],
                "base_value": row[4],
            }
            for row in rows
        ]


def interactive_shell(prog_name: str) -> None:
    console.print("[bold]spreadhist interactive mode[/bold]")
    console.print("Type a command such as [cyan]pnl --all[/cyan], [cyan]report summary[/cyan], or [cyan]help[/cyan].")
    console.print("Type [cyan]exit[/cyan] or press Ctrl+D/Ctrl+C to quit.")

    while True:
        try:
            raw_command = typer.prompt("spreadhist")
        except (EOFError, KeyboardInterrupt, click.Abort):
            console.print("\n[dim]Exiting.[/dim]")
            return

        command = raw_command.strip()
        if not command:
            continue
        if command.lower() in {"exit", "quit", "q"}:
            console.print("[dim]Exiting.[/dim]")
            return

        try:
            args = ["--help"] if command.lower() in {"help", "?"} else shlex.split(command)
        except ValueError as exc:
            console.print(f"[red]Could not parse command:[/red] {exc}")
            continue

        try:
            app(args=args, prog_name=prog_name, standalone_mode=False)
        except click.ClickException as exc:
            exc.show()
        except click.Abort:
            console.print("\n[dim]Command aborted.[/dim]")
        except KeyboardInterrupt:
            console.print("\n[dim]Command interrupted.[/dim]")
        except SystemExit as exc:
            if exc.code not in {0, None}:
                console.print(f"[red]Command exited with status {exc.code}.[/red]")


@app.callback(invoke_without_command=True, no_args_is_help=False)
def main(
    ctx: typer.Context,
    db: str = typer.Option(
        str(Path("spreadhist.db").resolve()),
        "--db",
        help="SQLite database path.",
    )
) -> None:
    STATE["db_path"] = db
    if ctx.invoked_subcommand is None:
        interactive_shell(ctx.info_name or "spreadhist")


def get_db() -> Database:
    return Database(STATE["db_path"])


def flatten_orders(order: dict[str, Any], parent_order_id: Optional[str] = None) -> list[dict[str, Any]]:
    record = dict(order)
    record["parent_order_id"] = parent_order_id
    legs = record.pop("legs", None) or []
    flattened = [record]
    for leg in legs:
        flattened.extend(flatten_orders(leg, parent_order_id=record.get("id")))
    return flattened


def load_order_events(db: Database) -> list[OrderLegEvent]:
    events: list[OrderLegEvent] = []
    for row in db.fetch_raw_orders():
        symbol = row.get("symbol")
        contract = parse_option_symbol(symbol or "")
        if not contract:
            continue
        status = str(row.get("status") or "").lower()
        filled_qty = parse_int(row.get("filled_qty"))
        qty = parse_int(row.get("qty"))
        if status not in ORDER_FINAL_STATUSES or filled_qty <= 0:
            continue
        events.append(
            OrderLegEvent(
                order_id=row.get("id"),
                parent_order_id=row.get("parent_order_id"),
                client_order_id=row.get("client_order_id"),
                symbol=symbol,
                side=str(row.get("side") or "").lower(),
                position_intent=(str(row.get("position_intent") or "").lower() or None),
                qty=qty,
                filled_qty=filled_qty,
                price=parse_decimal(row.get("filled_avg_price") or row.get("limit_price")),
                filled_at=parse_iso_datetime(row.get("filled_at")),
                submitted_at=parse_iso_datetime(row.get("submitted_at")),
                status=status,
                order_class=str(row.get("order_class") or "").lower(),
                contract=contract,
                raw=row,
            )
        )
    events.sort(key=lambda item: (item.timestamp, item.order_id))
    return events


def build_grouped_parent_events(events: list[OrderLegEvent]) -> tuple[list[GroupedSpreadEvent], set[str]]:
    by_parent: dict[str, list[OrderLegEvent]] = {}
    for event in events:
        if event.parent_order_id:
            by_parent.setdefault(event.parent_order_id, []).append(event)
    grouped: list[GroupedSpreadEvent] = []
    consumed: set[str] = set()
    for parent_id, legs in by_parent.items():
        if len(legs) != 2:
            continue
        if any(leg.order_class != "mleg" for leg in legs):
            continue
        first, second = legs
        contracts = [first.contract, second.contract]
        if len({c.underlying for c in contracts}) != 1:
            continue
        if len({c.option_type for c in contracts}) != 1:
            continue
        if len({c.strike for c in contracts}) != 1:
            continue
        if len({leg.filled_qty for leg in legs}) != 1:
            continue
        if first.contract.expiration == second.contract.expiration:
            continue
        intents = {leg.position_intent for leg in legs if leg.position_intent}
        if intents and intents.issubset(ORDER_OPEN_INTENTS):
            intent = "entry"
            long_leg = next((leg for leg in legs if leg.position_intent == "buy_to_open"), None)
            short_leg = next((leg for leg in legs if leg.position_intent == "sell_to_open"), None)
            if not long_leg or not short_leg:
                continue
        elif intents and intents.issubset(ORDER_CLOSE_INTENTS):
            intent = "exit"
            long_leg = next((leg for leg in legs if leg.position_intent == "sell_to_close"), None)
            short_leg = next((leg for leg in legs if leg.position_intent == "buy_to_close"), None)
            if not long_leg or not short_leg:
                continue
        else:
            continue
        grouped.append(
            GroupedSpreadEvent(
                event_id=f"parent::{parent_id}",
                parent_order_id=parent_id,
                timestamp=max(leg.timestamp for leg in legs),
                qty=long_leg.filled_qty,
                underlying=long_leg.contract.underlying,
                option_type=long_leg.contract.option_type,
                strike=long_leg.contract.strike,
                short_symbol=short_leg.symbol,
                short_expiration=short_leg.contract.expiration,
                short_price=short_leg.price,
                long_symbol=long_leg.symbol,
                long_expiration=long_leg.contract.expiration,
                long_price=long_leg.price,
                intent=intent,
                raw={"parent_order_id": parent_id, "legs": [leg.raw for leg in legs]},
            )
        )
        consumed.update({leg.order_id for leg in legs})
    grouped.sort(key=lambda item: (item.timestamp, item.parent_order_id))
    return grouped, consumed


def _signed_open_cash(event_type: str, price: Decimal, qty: int) -> Decimal:
    multiplier = Decimal(100) * Decimal(qty)
    if event_type == "open_long":
        return -(price * multiplier)
    if event_type == "open_short":
        return price * multiplier
    if event_type == "close_long":
        return price * multiplier
    if event_type == "close_short":
        return -(price * multiplier)
    return Decimal("0")


def infer_event_type(event: OrderLegEvent, open_spreads: list[SpreadState]) -> str:
    if event.position_intent == "buy_to_open":
        return "open_long"
    if event.position_intent == "sell_to_open":
        return "open_short"
    if event.position_intent == "sell_to_close":
        return "close_long"
    if event.position_intent == "buy_to_close":
        return "close_short"

    if event.side == "sell":
        if any(sp.long_symbol == event.symbol and sp.status in {"open", "broken", "resolved_unmatched"} for sp in open_spreads):
            return "close_long"
        return "open_short"
    if event.side == "buy":
        if any(sp.short_symbol == event.symbol and sp.status in {"open", "broken", "resolved_unmatched"} for sp in open_spreads):
            return "close_short"
        return "open_long"
    return "unknown"


def make_spread_id(*parts: str) -> str:
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:12]
    return f"cal_{digest}"


def match_open_spread(
    open_spreads: list[SpreadState],
    *,
    short_symbol: str,
    long_symbol: str,
    qty: int,
) -> Optional[SpreadState]:
    candidates = [
        spread
        for spread in open_spreads
        if spread.short_symbol == short_symbol
        and spread.long_symbol == long_symbol
        and spread.qty == qty
        and spread.status in {"open", "broken", "resolved_unmatched"}
    ]
    return min(candidates, key=lambda s: s.opened_at) if candidates else None


def reconstruct_spreads(db: Database) -> list[SpreadState]:
    order_events = load_order_events(db)
    grouped_parent_events, consumed_child_ids = build_grouped_parent_events(order_events)
    single_events = [event for event in order_events if event.order_id not in consumed_child_ids]

    actions: list[tuple[datetime, str, Any]] = []
    for grouped in grouped_parent_events:
        actions.append((grouped.timestamp, "grouped", grouped))
    for event in single_events:
        actions.append((event.timestamp, "single", event))
    actions.sort(key=lambda item: (item[0], item[1]))

    spreads: list[SpreadState] = []
    pending_openers: dict[tuple[str, str, str, int], list[OrderLegEvent]] = {}

    def open_spreads() -> list[SpreadState]:
        return [s for s in spreads if s.status in {"open", "broken", "resolved_unmatched"}]

    def create_spread_from_pair(long_event: OrderLegEvent, short_event: OrderLegEvent, source: str, group_mode: str) -> SpreadState:
        entry_debit = (long_event.price - short_event.price) * Decimal(100) * Decimal(long_event.filled_qty)
        spread_id = make_spread_id(
            long_event.order_id,
            short_event.order_id,
            long_event.symbol,
            short_event.symbol,
            str(long_event.timestamp),
        )
        spread = SpreadState(
            spread_id=spread_id,
            source=source,
            group_mode=group_mode,
            underlying=long_event.contract.underlying,
            option_type=long_event.contract.option_type,
            strike=long_event.contract.strike,
            qty=long_event.filled_qty,
            opened_at=max(long_event.timestamp, short_event.timestamp),
            short_symbol=short_event.symbol,
            short_expiration=short_event.contract.expiration,
            long_symbol=long_event.symbol,
            long_expiration=long_event.contract.expiration,
            entry_debit=entry_debit,
            cash_flow=_signed_open_cash("open_long", long_event.price, long_event.filled_qty)
            + _signed_open_cash("open_short", short_event.price, short_event.filled_qty),
            opening_order_id=long_event.parent_order_id or long_event.order_id,
            order_events=[
                {"kind": "open_long", "order_id": long_event.order_id, "symbol": long_event.symbol, "qty": long_event.filled_qty, "price": str(long_event.price)},
                {"kind": "open_short", "order_id": short_event.order_id, "symbol": short_event.symbol, "qty": short_event.filled_qty, "price": str(short_event.price)},
            ],
        )
        spreads.append(spread)
        return spread

    for _, kind, payload in actions:
        if kind == "grouped":
            grouped: GroupedSpreadEvent = payload
            if grouped.intent == "entry":
                long_contract = parse_option_symbol(grouped.long_symbol)
                short_contract = parse_option_symbol(grouped.short_symbol)
                if not long_contract or not short_contract:
                    continue
                long_event = OrderLegEvent(
                    order_id=f"{grouped.parent_order_id}:long",
                    parent_order_id=grouped.parent_order_id,
                    client_order_id=None,
                    symbol=grouped.long_symbol,
                    side="buy",
                    position_intent="buy_to_open",
                    qty=grouped.qty,
                    filled_qty=grouped.qty,
                    price=grouped.long_price,
                    filled_at=grouped.timestamp,
                    submitted_at=grouped.timestamp,
                    status="filled",
                    order_class="mleg",
                    contract=long_contract,
                    raw=grouped.raw,
                )
                short_event = OrderLegEvent(
                    order_id=f"{grouped.parent_order_id}:short",
                    parent_order_id=grouped.parent_order_id,
                    client_order_id=None,
                    symbol=grouped.short_symbol,
                    side="sell",
                    position_intent="sell_to_open",
                    qty=grouped.qty,
                    filled_qty=grouped.qty,
                    price=grouped.short_price,
                    filled_at=grouped.timestamp,
                    submitted_at=grouped.timestamp,
                    status="filled",
                    order_class="mleg",
                    contract=short_contract,
                    raw=grouped.raw,
                )
                spread = create_spread_from_pair(long_event, short_event, source="mleg", group_mode="parent")
                spread.opening_order_id = grouped.parent_order_id
                continue

            match = match_open_spread(
                open_spreads(),
                short_symbol=grouped.short_symbol,
                long_symbol=grouped.long_symbol,
                qty=grouped.qty,
            )
            if not match:
                continue
            match.cash_flow += _signed_open_cash("close_long", grouped.long_price, grouped.qty)
            match.cash_flow += _signed_open_cash("close_short", grouped.short_price, grouped.qty)
            match.close_value += (grouped.long_price - grouped.short_price) * Decimal(100) * Decimal(grouped.qty)
            match.closed_at = grouped.timestamp
            match.status = "closed"
            match.closing_order_ids.append(grouped.parent_order_id)
            match.long_closed_qty = grouped.qty
            match.short_closed_qty = grouped.qty
            match.long_resolved = "closed"
            match.short_resolved = "closed"
            match.order_events.extend(
                [
                    {"kind": "close_long", "order_id": f"{grouped.parent_order_id}:long", "symbol": grouped.long_symbol, "qty": grouped.qty, "price": str(grouped.long_price)},
                    {"kind": "close_short", "order_id": f"{grouped.parent_order_id}:short", "symbol": grouped.short_symbol, "qty": grouped.qty, "price": str(grouped.short_price)},
                ]
            )
            continue

        event: OrderLegEvent = payload
        event_type = infer_event_type(event, open_spreads())
        key = (event.contract.underlying, event.contract.option_type, str(event.contract.strike), event.filled_qty)

        if event_type in {"open_long", "open_short"}:
            opposites = pending_openers.get(key, [])
            match_idx: Optional[int] = None
            matched_event: Optional[OrderLegEvent] = None
            for idx, candidate in enumerate(opposites):
                if candidate.symbol == event.symbol:
                    continue
                if abs((candidate.timestamp - event.timestamp).total_seconds()) > 15 * 60:
                    continue
                if candidate.contract.expiration == event.contract.expiration:
                    continue
                if event_type == "open_long" and candidate.side == "sell" and event.contract.expiration > candidate.contract.expiration:
                    match_idx = idx
                    matched_event = candidate
                    break
                if event_type == "open_short" and candidate.side == "buy" and candidate.contract.expiration > event.contract.expiration:
                    match_idx = idx
                    matched_event = candidate
                    break
            if matched_event is not None and match_idx is not None:
                opposites.pop(match_idx)
                if event_type == "open_long":
                    create_spread_from_pair(event, matched_event, source="inferred", group_mode="infer")
                else:
                    create_spread_from_pair(matched_event, event, source="inferred", group_mode="infer")
            else:
                pending_openers.setdefault(key, []).append(event)
            continue

        if event_type == "close_long":
            candidates = [
                spread
                for spread in open_spreads()
                if spread.long_symbol == event.symbol and spread.qty == event.filled_qty
            ]
            if candidates:
                spread = min(candidates, key=lambda s: s.opened_at)
                spread.cash_flow += _signed_open_cash("close_long", event.price, event.filled_qty)
                spread.close_value += _signed_open_cash("close_long", event.price, event.filled_qty)
                spread.long_closed_qty = event.filled_qty
                spread.long_resolved = "closed"
                spread.order_events.append(
                    {"kind": "close_long", "order_id": event.order_id, "symbol": event.symbol, "qty": event.filled_qty, "price": str(event.price)}
                )
                spread.closing_order_ids.append(event.order_id)
                if spread.short_closed_qty >= spread.qty or spread.short_resolved in {"expired", "assigned", "exercised", "closed"}:
                    spread.closed_at = event.timestamp
                    spread.status = "closed"
                else:
                    spread.status = "broken"
            continue

        if event_type == "close_short":
            candidates = [
                spread
                for spread in open_spreads()
                if spread.short_symbol == event.symbol and spread.qty == event.filled_qty
            ]
            if candidates:
                spread = min(candidates, key=lambda s: s.opened_at)
                spread.cash_flow += _signed_open_cash("close_short", event.price, event.filled_qty)
                spread.close_value += _signed_open_cash("close_short", event.price, event.filled_qty)
                spread.short_closed_qty = event.filled_qty
                spread.short_resolved = "closed"
                spread.order_events.append(
                    {"kind": "close_short", "order_id": event.order_id, "symbol": event.symbol, "qty": event.filled_qty, "price": str(event.price)}
                )
                spread.closing_order_ids.append(event.order_id)
                if spread.long_closed_qty >= spread.qty or spread.long_resolved in {"expired", "assigned", "exercised", "closed"}:
                    spread.closed_at = event.timestamp
                    spread.status = "closed"
                else:
                    spread.status = "broken"
            continue

    positions_by_symbol = {pos.get("symbol"): pos for pos in db.fetch_raw_positions() if parse_option_symbol(pos.get("symbol") or "")}
    activities = db.fetch_raw_activities()
    option_activities = [activity for activity in activities if (activity.get("activity_type") or "") in OPTION_ACTIVITY_TYPES]
    option_activities.sort(key=lambda act: parse_iso_datetime(act.get("transaction_time") or act.get("date")))

    for spread in spreads:
        for activity in option_activities:
            symbol = activity.get("symbol")
            activity_ts = parse_iso_datetime(activity.get("transaction_time") or activity.get("date"))
            if activity_ts < spread.opened_at:
                continue
            activity_type = activity.get("activity_type")
            if symbol == spread.short_symbol:
                if activity_type == "OPEXP":
                    spread.short_resolved = "expired"
                elif activity_type == "OPASN":
                    spread.short_resolved = "assigned"
                    spread.assignment_or_exercise = True
                    spread.incomplete_economics = True
                elif activity_type in {"OPXRC", "OPEXC"}:
                    spread.short_resolved = "exercised"
                    spread.assignment_or_exercise = True
                    spread.incomplete_economics = True
            if symbol == spread.long_symbol:
                if activity_type == "OPEXP":
                    spread.long_resolved = "expired"
                elif activity_type == "OPASN":
                    spread.long_resolved = "assigned"
                    spread.assignment_or_exercise = True
                    spread.incomplete_economics = True
                elif activity_type in {"OPXRC", "OPEXC"}:
                    spread.long_resolved = "exercised"
                    spread.assignment_or_exercise = True
                    spread.incomplete_economics = True
        if spread.assignment_or_exercise:
            spread.add_note("Assignment/exercise detected. Reported P&L excludes the linked underlying OPTRD cash flows.")
        if spread.short_resolved == "expired" or spread.long_resolved == "expired":
            spread.add_note("One leg expired based on account activities.")
        if (spread.short_resolved in {"expired", "assigned", "exercised", "closed"}) and (
            spread.long_resolved in {"expired", "assigned", "exercised", "closed"}
        ):
            if not spread.closed_at:
                spread.closed_at = max(spread.opened_at, datetime.now(tz=timezone.utc))
                spread.status = "closed"
        spread.mark_status_from_positions(positions_by_symbol)

    spreads.sort(key=lambda item: (item.opened_at, item.spread_id))
    return spreads


def positions_by_symbol(db: Database) -> dict[str, dict[str, Any]]:
    return {row.get("symbol"): row for row in db.fetch_raw_positions()}


def window_start_iso(window: DateWindow) -> Optional[str]:
    return window.start.isoformat() if window.start else None


def window_end_iso(window: DateWindow) -> Optional[str]:
    return window.end_exclusive.isoformat() if window.end_exclusive else None


def decimal_to_str(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}"


def optional_decimal_to_str(value: Optional[Decimal]) -> str:
    return decimal_to_str(value) if value is not None else "-"


def optional_percent_to_str(value: Optional[Decimal]) -> str:
    return f"{value.quantize(Decimal('0.01'))}%" if value is not None else "-"


def percent_change(change: Decimal, basis: Decimal) -> Optional[Decimal]:
    if basis == 0:
        return None
    return (change / basis) * Decimal(100)


def portfolio_current_value(db: Database, window: DateWindow) -> Optional[Decimal]:
    rows = [
        row
        for row in db.fetch_portfolio_history()
        if datetime_in_window(parse_portfolio_timestamp(row.get("ts")), window)
    ]
    for row in reversed(rows):
        equity = row.get("equity")
        if equity not in {None, ""}:
            return parse_decimal(equity)
        profit_loss = row.get("profit_loss")
        if profit_loss not in {None, ""}:
            return PORTFOLIO_INITIAL_VALUE + parse_decimal(profit_loss)
    return None


def render_portfolio_table(db: Database, window: DateWindow) -> None:
    current_value = portfolio_current_value(db, window)
    change = current_value - PORTFOLIO_INITIAL_VALUE if current_value is not None else None
    table = Table(box=box.SIMPLE_HEAVY, title="Portfolio")
    table.add_column("Initial Value", justify="right")
    table.add_column("Current Value", justify="right")
    table.add_column("P&L", justify="right")
    table.add_column("% Change", justify="right")
    table.add_row(
        decimal_to_str(PORTFOLIO_INITIAL_VALUE),
        optional_decimal_to_str(current_value),
        optional_decimal_to_str(change),
        optional_percent_to_str(percent_change(change, PORTFOLIO_INITIAL_VALUE) if change is not None else None),
    )
    console.print(table)


def leg_performances(spread: SpreadState, positions_by_symbol: dict[str, dict[str, Any]]) -> list[LegPerformance]:
    legs: dict[str, dict[str, Any]] = {
        "Short": {
            "symbol": spread.short_symbol,
            "status": "Open" if spread.short_resolved is None else spread.short_resolved.capitalize(),
            "open_cash": Decimal("0"),
            "close_cash": Decimal("0"),
        },
        "Long": {
            "symbol": spread.long_symbol,
            "status": "Open" if spread.long_resolved is None else spread.long_resolved.capitalize(),
            "open_cash": Decimal("0"),
            "close_cash": Decimal("0"),
        },
    }
    event_cash_types = {
        "open_long": ("Long", "open_cash"),
        "open_short": ("Short", "open_cash"),
        "close_long": ("Long", "close_cash"),
        "close_short": ("Short", "close_cash"),
    }
    for event in spread.order_events:
        kind = str(event.get("kind", ""))
        cash_target = event_cash_types.get(kind)
        if not cash_target:
            continue
        label, cash_key = cash_target
        qty = parse_int(event.get("qty"))
        price = parse_decimal(event.get("price"))
        legs[label][cash_key] += _signed_open_cash(kind, price, qty)

    performances: list[LegPerformance] = []
    for label in ("Short", "Long"):
        symbol = str(legs[label]["symbol"])
        position = positions_by_symbol.get(symbol)
        current_value = parse_decimal(position.get("market_value")) if position else Decimal("0")
        performances.append(
            LegPerformance(
                label=label,
                symbol=symbol,
                status=str(legs[label]["status"]),
                open_cash=legs[label]["open_cash"],
                close_cash=legs[label]["close_cash"],
                current_value=current_value,
            )
        )
    return performances


def render_spreads_table(spreads: list[SpreadState]) -> None:
    table = Table(box=box.SIMPLE_HEAVY)
    table.add_column("Spread ID")
    table.add_column("Underlying")
    table.add_column("Type")
    table.add_column("Strike", justify="right")
    table.add_column("Opened")
    table.add_column("Closed")
    table.add_column("Status")
    table.add_column("Realized P&L", justify="right")
    table.add_column("% Change", justify="right")
    for spread in spreads:
        table.add_row(
            spread.spread_id,
            spread.underlying,
            spread.option_type,
            decimal_to_str(spread.strike),
            spread.opened_at.date().isoformat(),
            spread.closed_at.date().isoformat() if spread.closed_at else "-",
            spread.status,
            decimal_to_str(spread.realized_pnl),
            optional_percent_to_str(spread.realized_pnl_percent),
        )
    console.print(table)


@app.command()
def sync(
    since: Optional[str] = typer.Option(None, help="Start window: 11d, 3w, 5m, 10y, or 2026-02-11."),
    until: Optional[str] = typer.Option(None, help="Optional inclusive end date/date-time, e.g. 2026-03-03."),
    ytd: bool = typer.Option(False, "--ytd", help="Use the year-to-date window."),
    all_time: bool = typer.Option(False, "--all", help="Sync all available history."),
    paper: bool = typer.Option(True, "--paper/--live", help="Use the paper trading endpoint."),
    api_key: Optional[str] = typer.Option(None, envvar="ALPACA_API_KEY", help="Alpaca API key."),
    api_secret: Optional[str] = typer.Option(None, envvar="ALPACA_SECRET_KEY", help="Alpaca API secret."),
) -> None:
    """Sync orders, activities, positions, and portfolio history into SQLite."""
    if not api_key or not api_secret:
        raise typer.BadParameter("Provide ALPACA_API_KEY and ALPACA_SECRET_KEY, or pass --api-key/--api-secret.")

    window = resolve_date_window(since=since, until=until, ytd=ytd, all_time=all_time)
    print_final_timeframe(window, basis="sync API request window")
    db = get_db()
    client = AlpacaClient(api_key=api_key, api_secret=api_secret, paper=paper)

    orders = client.fetch_orders(start=window_start_iso(window), end=window_end_iso(window))
    flattened_orders: list[dict[str, Any]] = []
    for order in orders:
        flattened_orders.extend(flatten_orders(order))
    activities = client.fetch_account_activities(start=window_start_iso(window), end=window_end_iso(window))
    positions = client.fetch_positions()
    portfolio_history = client.fetch_portfolio_history(start=window_start_iso(window), end=window_end_iso(window))

    order_count = db.upsert_orders(flattened_orders)
    activity_count = db.upsert_activities(activities)
    position_count = db.replace_positions(positions)
    portfolio_count = db.replace_portfolio_history(portfolio_history)
    db.upsert_metadata("last_sync_at", datetime.now(tz=timezone.utc).isoformat())

    table = Table(title=f"Sync complete ({window.label})", box=box.SIMPLE)
    table.add_column("Dataset")
    table.add_column("Rows", justify="right")
    table.add_row("orders", str(order_count))
    table.add_row("activities", str(activity_count))
    table.add_row("positions", str(position_count))
    table.add_row("portfolio_history", str(portfolio_count))
    console.print(table)


@app.command()
def spreads(
    status: str = typer.Option("all", help="Filter by status: all, open, closed, broken."),
    underlying: Optional[str] = typer.Option(None, help="Filter by underlying symbol."),
    since: Optional[str] = typer.Option(None, help="Start window: 11d, 3w, 5m, 10y, or 2026-02-11."),
    until: Optional[str] = typer.Option(None, help="Optional inclusive end date/date-time, e.g. 2026-03-03."),
    ytd: bool = typer.Option(False, "--ytd", help="Use the year-to-date window."),
    all_time: bool = typer.Option(False, "--all", help="Show spreads from all time."),
) -> None:
    """List reconstructed 2-leg calendar spreads."""
    window = resolve_date_window(since=since, until=until, ytd=ytd, all_time=all_time)
    print_final_timeframe(window, basis="spread overlap")
    db = get_db()
    spread_rows = filter_spreads_by_window(reconstruct_spreads(db), window, mode="overlap")
    if underlying:
        spread_rows = [s for s in spread_rows if s.underlying.upper() == underlying.upper()]
    if status != "all":
        spread_rows = [s for s in spread_rows if s.status == status]
    render_spreads_table(spread_rows)


@app.command(name="open")
def open_spreads_cmd(
    underlying: Optional[str] = typer.Option(None, help="Filter by underlying symbol."),
    since: Optional[str] = typer.Option(None, help="Start window based on spread open date: 11d, 3w, 5m, 10y, or 2026-02-11."),
    until: Optional[str] = typer.Option(None, help="Optional inclusive end date/date-time, e.g. 2026-03-03."),
    ytd: bool = typer.Option(False, "--ytd", help="Use the year-to-date window."),
    all_time: bool = typer.Option(False, "--all", help="Show all currently open spreads regardless of open date."),
) -> None:
    """Show currently open or broken calendar spreads."""
    window = resolve_date_window(since=since, until=until, ytd=ytd, all_time=all_time)
    print_final_timeframe(window, basis="spread open date")
    db = get_db()
    spread_rows = [s for s in reconstruct_spreads(db) if s.status in {"open", "broken", "resolved_unmatched"}]
    spread_rows = filter_spreads_by_window(spread_rows, window, mode="opened")
    if underlying:
        spread_rows = [s for s in spread_rows if s.underlying.upper() == underlying.upper()]
    positions = positions_by_symbol(db)
    table = Table(box=box.SIMPLE_HEAVY)
    table.add_column("Spread ID")
    table.add_column("Underlying")
    table.add_column("Open Date")
    table.add_column("Avg Open Fill", justify="right")
    table.add_column("Total Open", justify="right")
    table.add_column("Status")
    table.add_column("Current Value", justify="right")
    table.add_column("P&L to Date", justify="right")
    table.add_column("% Change", justify="right")
    for spread in spread_rows:
        current_value = spread.current_value(positions)
        pnl_to_date = spread.cash_flow + current_value
        table.add_row(
            spread.spread_id,
            spread.underlying,
            spread.opened_at.date().isoformat(),
            decimal_to_str(spread.avg_open_fill_price),
            decimal_to_str(spread.total_open_price),
            spread.status,
            decimal_to_str(current_value),
            decimal_to_str(pnl_to_date),
            optional_percent_to_str(percent_change(pnl_to_date, abs(spread.entry_debit))),
        )
    console.print(table)


@app.command()
def closed(
    month: Optional[str] = typer.Option(None, help="Optional YYYY-MM filter based on opened month or closed month."),
    since: Optional[str] = typer.Option(None, help="Start window based on spread close date: 11d, 3w, 5m, 10y, or 2026-02-11."),
    until: Optional[str] = typer.Option(None, help="Optional inclusive end date/date-time, e.g. 2026-03-03."),
    ytd: bool = typer.Option(False, "--ytd", help="Use the year-to-date window."),
    all_time: bool = typer.Option(False, "--all", help="Show closed spreads from all time."),
) -> None:
    """Show closed calendar spreads."""
    window = resolve_date_window(since=since, until=until, ytd=ytd, all_time=all_time)
    print_final_timeframe(window, basis="spread close date")
    db = get_db()
    spread_rows = [s for s in reconstruct_spreads(db) if s.status == "closed"]
    spread_rows = filter_spreads_by_window(spread_rows, window, mode="closed")
    if month:
        spread_rows = [
            s
            for s in spread_rows
            if s.opened_at.strftime("%Y-%m") == month or (s.closed_at and s.closed_at.strftime("%Y-%m") == month)
        ]
    render_spreads_table(spread_rows)


@app.command()
def spread(spread_id: str = typer.Argument(..., help="Spread identifier from the spreads command.")) -> None:
    """Show a detailed view of one reconstructed calendar spread."""
    db = get_db()
    spread_rows = reconstruct_spreads(db)
    selected = next((s for s in spread_rows if s.spread_id == spread_id), None)
    if not selected:
        raise typer.BadParameter(f"Spread {spread_id} was not found.")

    console.print(f"[bold]{selected.spread_id}[/bold]")
    console.print(f"  Underlying: {selected.underlying}")
    console.print(f"        Type: {selected.option_type}")
    console.print(f"      Strike: {decimal_to_str(selected.strike)}")
    console.print(f"   Open Date: {selected.opened_at.isoformat()}")
    console.print(f"  Close Date: {selected.closed_at.isoformat() if selected.closed_at else '-'}")
    console.print(f"      Status: {selected.status}")
    console.print(f"    Avg Open: {decimal_to_str(selected.avg_open_fill_price)}")
    console.print(f"   Avg Close: {optional_decimal_to_str(selected.avg_close_fill_price)}")
    console.print(f"  Total Open: {decimal_to_str(selected.entry_debit)}")
    console.print(f" Total Close: {optional_decimal_to_str(selected.total_close_price)}")
    console.print(f"Realized P&L: {decimal_to_str(selected.realized_pnl)}")
    console.print(f"   % Change: {optional_percent_to_str(selected.realized_pnl_percent)}")

    positions = positions_by_symbol(db)
    legs_table = Table(title="Legs", box=box.SIMPLE)
    legs_table.add_column("Leg")
    legs_table.add_column("Symbol")
    legs_table.add_column("Strike", justify="right")
    legs_table.add_column("Expiration")
    legs_table.add_column("Status")
    legs_table.add_column("Open Cash", justify="right")
    legs_table.add_column("Close Cash", justify="right")
    legs_table.add_column("Current Value", justify="right")
    legs_table.add_column("P&L", justify="right")
    legs_table.add_column("% Change", justify="right")

    expirations = {"Short": selected.short_expiration, "Long": selected.long_expiration}
    for leg in leg_performances(selected, positions):
        legs_table.add_row(
            leg.label,
            leg.symbol,
            decimal_to_str(selected.strike),
            expirations[leg.label].isoformat(),
            leg.status,
            decimal_to_str(leg.open_cash),
            decimal_to_str(leg.close_cash),
            decimal_to_str(leg.current_value),
            decimal_to_str(leg.pnl),
            optional_percent_to_str(leg.percent_change),
        )
    console.print(legs_table)

    events_table = Table(title="Events", box=box.SIMPLE)
    events_table.add_column("Kind")
    events_table.add_column("Order ID")
    events_table.add_column("Symbol")
    events_table.add_column("Qty", justify="right")
    events_table.add_column("Price", justify="right")
    events_table.add_column("Total", justify="right")

    for event in selected.order_events:
        qty = parse_int(event.get("qty"))
        price = parse_decimal(event.get("price"))
        total = price * Decimal(100) * Decimal(qty) if qty and str(event.get("price", "")) not in {"", "None"} else None

        events_table.add_row(
            str(event.get("kind", "")),
            str(event.get("order_id", "")),
            str(event.get("symbol", "")),
            str(event.get("qty", "")),
            str(event.get("price", "")),
            decimal_to_str(total) if total is not None else "-",
        )

    console.print(events_table)


def render_pnl_tables(db: Database, window: DateWindow, *, period: str, group_by: str) -> None:
    spread_rows = filter_spreads_by_window(reconstruct_spreads(db), window, mode="anchor")
    buckets: dict[str, dict[str, Decimal | int]] = {}
    for spread in spread_rows:
        if group_by == "underlying":
            key = spread.underlying
        else:
            anchor = spread.closed_at or spread.opened_at
            key = anchor.strftime("%Y-%m-%d") if period == "day" else anchor.strftime("%Y-%m")
        bucket = buckets.setdefault(key, {"pnl": Decimal("0"), "basis": Decimal("0"), "count": 0})
        bucket["pnl"] = bucket["pnl"] + spread.realized_pnl  # type: ignore[operator]
        bucket["basis"] = bucket["basis"] + abs(spread.entry_debit)  # type: ignore[operator]
        bucket["count"] = int(bucket["count"]) + 1  # type: ignore[arg-type]

    table = Table(box=box.SIMPLE_HEAVY, title=f"P&L ({window.label})")
    table.add_column("Bucket")
    table.add_column("Spreads", justify="right")
    table.add_column("Realized P&L", justify="right")
    table.add_column("Spread % Change", justify="right")
    for key in sorted(buckets):
        bucket = buckets[key]
        pnl_value = bucket["pnl"]
        basis_value = bucket["basis"]
        table.add_row(
            key,
            str(bucket["count"]),
            decimal_to_str(pnl_value),
            optional_percent_to_str(percent_change(pnl_value, basis_value)),
        )
    console.print(table)
    render_portfolio_table(db, window)


@app.command()
def pnl(
    period: str = typer.Option("month", help="Grouping period: day or month."),
    group_by: str = typer.Option("period", help="Grouping dimension: period or underlying."),
    since: Optional[str] = typer.Option(None, help="Start window: 11d, 3w, 5m, 10y, or 2026-02-11."),
    until: Optional[str] = typer.Option(None, help="Optional inclusive end date/date-time, e.g. 2026-03-03."),
    ytd: bool = typer.Option(False, "--ytd", help="Use the year-to-date window."),
    all_time: bool = typer.Option(False, "--all", help="Show P&L across all time."),
) -> None:
    """Summarize realized P&L across reconstructed spreads."""
    window = resolve_date_window(since=since, until=until, ytd=ytd, all_time=all_time)
    print_final_timeframe(window, basis="closed date, or open date when still open")
    db = get_db()
    render_pnl_tables(db, window, period=period, group_by=group_by)


@app.command()
def reconcile() -> None:
    """Run consistency checks on reconstructed calendar spreads."""
    db = get_db()
    spread_rows = reconstruct_spreads(db)
    positions = positions_by_symbol(db)
    issues: list[tuple[str, str]] = []
    for spread in spread_rows:
        if spread.long_expiration <= spread.short_expiration:
            issues.append((spread.spread_id, "Long leg expiration is not later than short leg expiration."))
        if spread.assignment_or_exercise:
            issues.append((spread.spread_id, "Assignment/exercise detected; realized P&L is incomplete without the linked underlying flows."))
        short_open = spread.short_symbol in positions
        long_open = spread.long_symbol in positions
        if spread.status == "closed" and (short_open or long_open):
            issues.append((spread.spread_id, "Spread is marked closed but at least one leg is still in current positions."))
        if spread.status in {"open", "broken", "resolved_unmatched"} and not (short_open or long_open):
            issues.append((spread.spread_id, "Spread is open-like but neither leg appears in current positions."))
        if spread.group_mode == "infer":
            issues.append((spread.spread_id, "This spread was inferred from separate orders rather than a native MLeg parent order."))

    if not issues:
        console.print("[green]No reconciliation issues found.[/green]")
        return

    table = Table(box=box.SIMPLE_HEAVY)
    table.add_column("Spread ID")
    table.add_column("Issue")
    for spread_id, issue in issues:
        table.add_row(spread_id, issue)
    console.print(table)


@app.command()
def report(
    kind: str = typer.Argument("monthly", help="daily, monthly, or summary"),
    since: Optional[str] = typer.Option(None, help="Start window: 11d, 3w, 5m, 10y, or 2026-02-11."),
    until: Optional[str] = typer.Option(None, help="Optional inclusive end date/date-time, e.g. 2026-03-03."),
    ytd: bool = typer.Option(False, "--ytd", help="Use the year-to-date window."),
    all_time: bool = typer.Option(False, "--all", help="Run the report across all time."),
) -> None:
    """Generate a concise strategy report."""
    window = resolve_date_window(since=since, until=until, ytd=ytd, all_time=all_time)
    print_final_timeframe(window, basis="closed date, or open date when still open")
    db = get_db()
    all_spreads = reconstruct_spreads(db)
    spread_rows = filter_spreads_by_window(all_spreads, window, mode="anchor")
    open_rows = filter_spreads_by_window(
        [s for s in all_spreads if s.status in {"open", "broken", "resolved_unmatched"}],
        window,
        mode="opened",
    )
    closed_rows = [s for s in spread_rows if s.status == "closed"]
    total_realized = sum((s.realized_pnl for s in closed_rows), Decimal("0"))
    total_basis = sum((abs(s.entry_debit) for s in closed_rows), Decimal("0"))
    wins = [s for s in closed_rows if s.realized_pnl > 0]
    losses = [s for s in closed_rows if s.realized_pnl < 0]
    avg_winner = (sum((s.realized_pnl for s in wins), Decimal("0")) / Decimal(len(wins))) if wins else Decimal("0")
    avg_loser = (sum((s.realized_pnl for s in losses), Decimal("0")) / Decimal(len(losses))) if losses else Decimal("0")
    winner_pct_values = [s.realized_pnl_percent for s in wins if s.realized_pnl_percent is not None]
    loser_pct_values = [s.realized_pnl_percent for s in losses if s.realized_pnl_percent is not None]
    avg_winner_percent = (
        sum(winner_pct_values, Decimal("0")) / Decimal(len(winner_pct_values)) if winner_pct_values else None
    )
    avg_loser_percent = (
        sum(loser_pct_values, Decimal("0")) / Decimal(len(loser_pct_values)) if loser_pct_values else None
    )

    console.print(f"[bold]Calendar spread report ({kind}, {window.label})[/bold]")
    console.print(f"Total reconstructed spreads: {len(spread_rows)}")
    console.print(f"Open/broken spreads: {len(open_rows)}")
    console.print(f"Closed spreads: {len(closed_rows)}")
    console.print(f"Win rate: {(len(wins) / len(closed_rows) * 100):.1f}%" if closed_rows else "Win rate: n/a")
    console.print(f"Realized P&L: {decimal_to_str(total_realized)}")
    console.print(f"Spread % Change: {optional_percent_to_str(percent_change(total_realized, total_basis))}")
    console.print(f"Average winner: {decimal_to_str(avg_winner)}")
    console.print(f"Average winner % Change: {optional_percent_to_str(avg_winner_percent)}")
    console.print(f"Average loser: {decimal_to_str(avg_loser)}")
    console.print(f"Average loser % Change: {optional_percent_to_str(avg_loser_percent)}")
    if kind not in {"daily", "monthly"}:
        render_portfolio_table(db, window)

    if kind in {"daily", "monthly"}:
        group_period = "day" if kind == "daily" else "month"
        render_pnl_tables(db, window, period=group_period, group_by="period")

    if open_rows:
        console.print("\n[bold]Open or broken spreads[/bold]")
        render_spreads_table(open_rows[:10])


@app.command()
def export(
    dataset: str = typer.Argument(..., help="spreads or pnl"),
    fmt: str = typer.Option("csv", "--format", help="csv or json"),
    output: str = typer.Option(..., help="Output file path."),
    since: Optional[str] = typer.Option(None, help="Start window: 11d, 3w, 5m, 10y, or 2026-02-11."),
    until: Optional[str] = typer.Option(None, help="Optional inclusive end date/date-time, e.g. 2026-03-03."),
    ytd: bool = typer.Option(False, "--ytd", help="Use the year-to-date window."),
    all_time: bool = typer.Option(False, "--all", help="Export across all time."),
) -> None:
    """Export reconstructed spread data."""
    window = resolve_date_window(since=since, until=until, ytd=ytd, all_time=all_time)
    print_final_timeframe(window, basis="closed date, or open date when still open")
    db = get_db()
    spread_rows = filter_spreads_by_window(reconstruct_spreads(db), window, mode="anchor")

    rows: list[dict[str, Any]]
    if dataset == "spreads":
        positions = positions_by_symbol(db)
        rows = []
        for s in spread_rows:
            leg_returns = {leg.label: leg for leg in leg_performances(s, positions)}
            rows.append(
                {
                    "spread_id": s.spread_id,
                    "source": s.source,
                    "group_mode": s.group_mode,
                    "underlying": s.underlying,
                    "option_type": s.option_type,
                    "strike": decimal_to_str(s.strike),
                    "qty": s.qty,
                    "opened_at": s.opened_at.isoformat(),
                    "closed_at": s.closed_at.isoformat() if s.closed_at else None,
                    "status": s.status,
                    "short_symbol": s.short_symbol,
                    "long_symbol": s.long_symbol,
                    "entry_debit": decimal_to_str(s.entry_debit),
                    "realized_pnl": decimal_to_str(s.realized_pnl),
                    "realized_pnl_percent": optional_percent_to_str(s.realized_pnl_percent),
                    "short_leg_percent": optional_percent_to_str(leg_returns["Short"].percent_change),
                    "long_leg_percent": optional_percent_to_str(leg_returns["Long"].percent_change),
                    "notes": " | ".join(s.notes),
                }
            )
    elif dataset == "pnl":
        grouped: dict[str, dict[str, Decimal]] = {}
        for spread in spread_rows:
            key = (spread.closed_at or spread.opened_at).strftime("%Y-%m")
            bucket = grouped.setdefault(key, {"pnl": Decimal("0"), "basis": Decimal("0")})
            bucket["pnl"] += spread.realized_pnl
            bucket["basis"] += abs(spread.entry_debit)
        rows = [
            {
                "period": key,
                "realized_pnl": decimal_to_str(grouped[key]["pnl"]),
                "realized_pnl_percent": optional_percent_to_str(
                    percent_change(grouped[key]["pnl"], grouped[key]["basis"])
                ),
            }
            for key in sorted(grouped)
        ]
    else:
        raise typer.BadParameter("dataset must be spreads or pnl")

    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "json":
        out_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    elif fmt == "csv":
        if not rows:
            out_path.write_text("", encoding="utf-8")
        else:
            with out_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)
    else:
        raise typer.BadParameter("format must be csv or json")
    console.print(f"Wrote {len(rows)} rows to {out_path} ({window.label})")


if __name__ == "__main__":
    app()
