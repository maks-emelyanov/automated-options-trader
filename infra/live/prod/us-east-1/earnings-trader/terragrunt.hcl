include "root" {
  path   = find_in_parent_folders("root.hcl")
  expose = true
}

terraform {
  source = "${get_repo_root()}/infra/modules//scheduled-python-lambda"
}

inputs = {
  name                  = "earnings-trader"
  package_source_root   = "${get_repo_root()}/src"
  package_requirements_file = "../requirements/lambda/earnings-trader.txt"
  handler               = "trading.earnings_trader_lambda_handler.handler"
  package_source_files  = ["trading/__init__.py", "trading/earnings_trader.py", "trading/earnings_trader_lambda_handler.py", "trading/logging_utils.py"]
  python_version        = "3.12"
  memory_size           = 512
  timeout               = 900
  log_retention_in_days = 14

  # Runs once at 3:45 PM ET on weekdays. The Lambda still checks Tradier
  # session data so holidays and early closes fail safe.
  schedule_expression = "cron(45 15 ? * MON-FRI *)"
  schedule_timezone   = "America/New_York"

  environment_variables = {
    ALPHAVANTAGE_API_KEY = get_env("ALPHAVANTAGE_API_KEY")
    ALPACA_API_KEY       = get_env("ALPACA_API_KEY")
    ALPACA_SECRET_KEY    = get_env("ALPACA_SECRET_KEY")
    TRADIER_TOKEN        = get_env("TRADIER_TOKEN")
    MARKET_TIMEZONE      = "America/New_York"
    ACCOUNT_VALUE_FIELD  = "cash"
    BUDGET_MODE          = "per_symbol"
    DRY_RUN              = "false"
    MARKET_ORDER_SLIPPAGE_PCT = "0.10"
    MIN_NET_DEBIT        = "0.01"
    PCT_OF_AVAILABLE     = "0.06"
    STRIKE_WINDOW_PCT    = "0.10"
    USE_MID_DEBIT        = "false"
  }

  schedule_input = {
    source = "eventbridge-scheduler"
  }

  tags = include.root.locals.common_tags
}
