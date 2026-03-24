variable "name" {
  description = "Lambda function name."
  type        = string
}

variable "package_source_root" {
  description = "Absolute path to the directory containing the Lambda source files."
  type        = string
}

variable "package_requirements_file" {
  description = "Path to the requirements file relative to package_source_root."
  type        = string
  default     = "lambda-requirements.txt"
}

variable "handler" {
  description = "Lambda handler string in module.function format."
  type        = string
  default     = "trading.earnings_trader_lambda_handler.handler"
}

variable "package_source_files" {
  description = "Files to package into the Lambda zip, relative to package_source_root."
  type        = list(string)
}

variable "python_version" {
  description = "Python runtime version without the python prefix."
  type        = string
  default     = "3.12"
}

variable "memory_size" {
  description = "Lambda memory size in MB."
  type        = number
  default     = 2048
}

variable "timeout" {
  description = "Lambda timeout in seconds."
  type        = number
  default     = 900
}

variable "log_retention_in_days" {
  description = "CloudWatch log retention."
  type        = number
  default     = 14
}

variable "schedule_expression" {
  description = "EventBridge Scheduler cron or rate expression."
  type        = string
}

variable "schedule_timezone" {
  description = "Timezone for the schedule expression."
  type        = string
  default     = "America/New_York"
}

variable "schedule_input" {
  description = "JSON payload sent to the Lambda."
  type        = map(any)
  default     = {}
}

variable "environment_variables" {
  description = "Lambda environment variables."
  type        = map(string)
  default     = {}
}

variable "tags" {
  description = "Common AWS tags."
  type        = map(string)
  default     = {}
}
