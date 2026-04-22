include "root" {
  path   = find_in_parent_folders("root.hcl")
  expose = true
}

terraform {
  source = "${get_repo_root()}/infra/modules//scheduled-python-lambda"
}

inputs = {
  name                  = "close-options"
  package_source_root   = "${get_repo_root()}/src"
  package_requirements_file = "../requirements/lambda/close-options.txt"
  handler               = "trading.close_options_lambda_handler.handler"
  package_source_files  = ["trading/__init__.py", "trading/close_options.py", "trading/close_options_lambda_handler.py", "trading/logging_utils.py", "trading/retry_utils.py", "trading/tradier_market.py"]
  python_version        = "3.12"
  memory_size           = 256
  timeout               = 300
  log_retention_in_days = 14

  # Runs at 9:45 AM ET on weekdays. The Lambda itself checks Tradier session
  # data and only executes when the market has been open for about 15 minutes,
  # which keeps holidays and non-session invocations safe.
  schedule_expression = "cron(45 9 ? * MON-FRI *)"
  schedule_timezone   = "America/New_York"

  environment_variables = {
    ALPACA_API_KEY     = get_env("ALPACA_API_KEY")
    ALPACA_SECRET_KEY  = get_env("ALPACA_SECRET_KEY")
    TRADIER_TOKEN      = get_env("TRADIER_TOKEN")
    DRY_RUN            = "false"
    MARKET_TIMEZONE    = "America/New_York"
  }

  schedule_input = {
    source = "eventbridge-scheduler"
  }

  tags = merge(
    include.root.locals.common_tags,
    {
      Component = "close-options"
    },
  )
}
