# Reporting CLI

A Python CLI for **Alpaca paper-trading 2-leg calendar spread history**.

It syncs Alpaca paper-account orders, activities, positions, and portfolio history into a local SQLite database, then reconstructs **2-leg calendar spreads** as first-class objects.

## What this version handles well

- Alpaca **paper** trading accounts
- Native multi-leg options orders submitted with `order_class="mleg"`
- Inferred pairing of separately-submitted legs when they look like a standard calendar
- Spread-focused commands:
  - `sync`
  - `spreads`
  - `spread`
  - `open`
  - `closed`
  - `pnl`
  - `reconcile`
  - `report`
  - `export`

## Important limitations

This version is designed as a strong v1, not a full broker-grade accounting engine.

- It reconstructs spreads primarily from **filled order history**, with activities used for expiry / assignment / exercise flags.
- If a spread is exercised or assigned, the CLI flags it and notes that realized P&L is **incomplete** unless you also model the linked underlying `OPTRD` activity.
- Inferred pairing is heuristic-based and assumes a standard same-strike calendar opened close in time.
- Partial-fill-heavy workflows are not as robust as simple filled 1-lot / N-lot entries and exits.

## Why it uses a local ledger

Alpaca notes that position `avg_entry_price` and `cost_basis` can differ intraday vs. after the beginning-of-day sync, so this tool reconstructs strategy history from synced order/activity data instead of trusting current positions alone.

## Install

From the repository root, install the project dependencies with `uv`:

```bash
uv sync
```

If you prefer a standard virtual environment, install the project dependencies first:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
```

## Configure credentials

Set your Alpaca paper credentials as environment variables:

```bash
export ALPACA_API_KEY="your_key"
export ALPACA_SECRET_KEY="your_secret"
```

## Basic usage

Run without a subcommand to start an interactive prompt. It accepts the same commands shown below and keeps running until you type `exit` or press Ctrl+C/Ctrl+D:

```bash
uv run python reporting/cli.py
spreadhist: pnl --all
spreadhist: report summary
spreadhist: exit
```

Sync from Alpaca paper trading into SQLite:

```bash
uv run python reporting/cli.py sync --since 2026-01-01
```

List reconstructed spreads:

```bash
uv run python reporting/cli.py spreads
uv run python reporting/cli.py spreads --status open
uv run python reporting/cli.py spreads --underlying AAPL
```

Inspect one spread:

```bash
uv run python reporting/cli.py spread cal_123456789abc
```

Show open calendars:

```bash
uv run python reporting/cli.py open
```

Show closed calendars:

```bash
uv run python reporting/cli.py closed
```

P&L summary:

```bash
uv run python reporting/cli.py pnl --period month
uv run python reporting/cli.py pnl --group-by underlying
```

P&L tables include spread percent change versus opening debit basis and portfolio percent change versus the initial $100,000 value. Individual spread details include percent change for each leg.

Near-term open-spread review:

```bash
uv run python reporting/cli.py open --since 7d
```

Reconciliation checks:

```bash
uv run python reporting/cli.py reconcile
```

Strategy summary report:

```bash
uv run python reporting/cli.py report monthly
```

Export results:

```bash
uv run python reporting/cli.py export spreads --format csv --output exports/spreads.csv
uv run python reporting/cli.py export pnl --format json --output exports/pnl.json
```

If you activated a virtual environment instead of using `uv run`, replace `uv run python reporting/cli.py` with `python reporting/cli.py`.

## Date windows

Commands that accept `--since` can use absolute dates or relative windows:

```bash
uv run python reporting/cli.py sync --since 2026-01-01 --until 2026-03-31
uv run python reporting/cli.py pnl --since 30d
uv run python reporting/cli.py report summary --ytd
uv run python reporting/cli.py spreads --all
```

Relative windows support `d`, `w`, `m`, and `y`, such as `11d`, `3w`, `5m`, or `1y`.

## Database

By default the CLI writes to:

```text
./spreadhist.db
```

Override it with:

```bash
uv run python reporting/cli.py --db /path/to/spreadhist.db spreads
```

## Notes on Alpaca-specific behavior

- Alpaca paper trading runs at `paper-api.alpaca.markets`.
- Options are enabled by default in paper accounts.
- Alpaca supports native multi-leg options orders with `order_class="mleg"` and nested `legs`.
- Account activities are paginated with `page_token`.
- On paper, options non-trade activities can appear on the following day.
