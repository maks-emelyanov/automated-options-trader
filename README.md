# Trading Automation

Automated options-trading workflows packaged as AWS Lambda functions and deployed with Terragrunt.

This repository currently contains two scheduled workflows:

- [`src/trading/earnings_trader.py`](src/trading/earnings_trader.py) evaluates earnings-related calendar spread opportunities and is invoked through [`src/trading/earnings_trader_lambda_handler.py`](src/trading/earnings_trader_lambda_handler.py).
- [`src/trading/close_options.py`](src/trading/close_options.py) identifies open calendar spreads and submits close orders through [`src/trading/close_options_lambda_handler.py`](src/trading/close_options_lambda_handler.py).

Both Lambda handlers are scheduled during market hours but still verify Tradier market session data at runtime so they safely skip holidays, early closes, and off-hours invocations.

At a high level, the current runtime split is:

- Alpha Vantage for the earnings calendar
- Tradier for market session checks, stock quotes, option expirations, option chains, and historical market data
- Alpaca for account sizing data, paper order placement, and open-position inspection during close-outs

## Important Notice

This project is experimental trading infrastructure and is not investment advice. Review the strategy logic, risk controls, broker permissions, and deployment settings yourself before using it with any live capital.

## What the Repository Includes

- Python application code under `src/trading/`
- Separate Lambda dependency manifests under `requirements/lambda/`
- Terragrunt live configuration under `infra/live/`
- A reusable Terraform module for scheduled Lambda deployment under `infra/modules/scheduled-python-lambda/`

## Schedules

- `earnings-trader`: `3:45 PM` weekdays in `America/New_York`
- `close-options`: `9:45 AM` weekdays in `America/New_York`

Deployed schedule names:

- `earnings-trader-market-hours`
- `close-options-market-hours`

## Quick Start

This repo uses Python `3.12`. If you use `uv`, the included lockfile can drive local setup:

```bash
uv sync
```

If you prefer a standard virtual environment:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements/lambda/earnings-trader.txt
pip install -r requirements/lambda/close-options.txt
```

## Required Environment Variables

Export these before local runs or Terragrunt deployments:

```bash
export ALPHAVANTAGE_API_KEY="..."
export ALPACA_API_KEY="..."
export ALPACA_SECRET_KEY="..."
export TRADIER_TOKEN="..."
```

Both workflows now require all four values. Alpaca is used for account access and order placement, while Tradier is used for market clock/session checks and market data.

The earnings workflow also supports `MARKET_ORDER_SLIPPAGE_PCT` to apply a safety buffer when sizing market-entry calendar spreads. The production Terragrunt config currently sets this to `0.10`, meaning sizing assumes fills may be about 10% worse than the raw quoted debit estimate.

## Run Locally

Use the `src` layout when invoking the package directly:

```bash
PYTHONPATH=src python3 -m trading.earnings_trader_lambda_handler
PYTHONPATH=src python3 -m trading.close_options_lambda_handler
PYTHONPATH=src python3 scripts/trading_cli.py --help
```

## Deploy

From the repository root:

```bash
cd infra/live/prod/us-east-1/earnings-trader
terragrunt init
terragrunt plan
terragrunt apply

cd ../close-options
terragrunt init
terragrunt plan
terragrunt apply
```

## Packaging Notes

The Terraform module builds a ZIP artifact locally, uploads it to an S3 artifact bucket, and updates Lambda from that object. This avoids Lambda direct-upload size limits.

If Docker is available, packaging uses the AWS SAM Python `3.12` build image so compiled dependencies such as `numpy` and `pandas` match the Lambda runtime. Without Docker, packaging falls back to a local `pip install`, which is only safe when the local environment matches the Lambda runtime ABI.

## Infrastructure Notes

[`infra/live/root.hcl`](infra/live/root.hcl) currently uses a local Terraform backend so the repository works immediately for a single operator. Before using this repository in a team or CI environment, switch to a remote backend such as S3 plus state locking.

## Repository Layout

- `src/trading/`: application code and Lambda handlers
- `requirements/lambda/`: per-Lambda dependency manifests
- `scripts/`: local entrypoints and utilities
- `infra/modules/scheduled-python-lambda/`: reusable Terraform module for scheduled Python Lambdas
- `infra/live/prod/us-east-1/earnings-trader/`: production deployment for the earnings trader
- `infra/live/prod/us-east-1/close-options/`: production deployment for the close-options Lambda

## Logging

Both workflows now emit structured Python logs for:

- Lambda entry and scheduling decisions
- External API requests and responses
- Tradier market session and market-data operations
- Alpaca account, position, contract, and order operations
- Strategy evaluation and skip reasons

These logs are captured by AWS Lambda and appear in CloudWatch Logs.

## Security

- Do not commit `.env` files, AWS credentials, or broker API secrets.
- Rotate broker and market-data credentials before making a repository public if they were ever used in local shell history or shared terminals.
- Review CloudWatch log output before public demos to make sure it does not include any sensitive identifiers you would rather keep private.

## License

This repository is licensed under the GNU General Public License v3.0. See [LICENSE](LICENSE).
