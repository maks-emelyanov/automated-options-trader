terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

locals {
  package_dir          = "${path.module}/.dist"
  package_zip          = "${local.package_dir}/${var.name}.zip"
  artifact_bucket_name = "${var.name}-lambda-artifacts-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.name}"
  package_triggers = {
    requirements_hash = filesha256("${var.package_source_root}/${var.package_requirements_file}")
    build_script_hash = filesha256("${path.module}/build_lambda_package.sh")
    source_hashes = jsonencode({
      for relpath in var.package_source_files :
      relpath => filesha256("${var.package_source_root}/${relpath}")
    })
  }
  artifact_key      = "${var.name}/${sha256(jsonencode(local.package_triggers))}.zip"
  package_files_arg = join(" ", var.package_source_files)
}

resource "null_resource" "package" {
  triggers = local.package_triggers

  provisioner "local-exec" {
    command = "${path.module}/build_lambda_package.sh ${var.package_source_root} ${var.package_source_root}/${var.package_requirements_file} ${var.python_version} ${local.package_zip} ${local.package_files_arg}"
  }
}

resource "aws_s3_bucket" "artifacts" {
  bucket = local.artifact_bucket_name

  tags = var.tags
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_object" "package" {
  depends_on = [
    null_resource.package,
    aws_s3_bucket_public_access_block.artifacts,
    aws_s3_bucket_versioning.artifacts,
    aws_s3_bucket_server_side_encryption_configuration.artifacts,
  ]

  bucket = aws_s3_bucket.artifacts.id
  key    = local.artifact_key
  source = local.package_zip

  tags = var.tags
}

resource "aws_iam_role" "lambda" {
  name = "${var.name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.name}"
  retention_in_days = var.log_retention_in_days

  tags = var.tags
}

resource "aws_lambda_function" "this" {
  depends_on = [
    aws_s3_object.package,
    aws_cloudwatch_log_group.lambda,
  ]

  function_name     = var.name
  role              = aws_iam_role.lambda.arn
  runtime           = "python${var.python_version}"
  handler           = var.handler
  s3_bucket         = aws_s3_bucket.artifacts.id
  s3_key            = aws_s3_object.package.key
  s3_object_version = aws_s3_object.package.version_id
  source_code_hash  = base64sha256(jsonencode(local.package_triggers))
  timeout           = var.timeout
  memory_size       = var.memory_size

  environment {
    variables = var.environment_variables
  }

  tags = var.tags
}

resource "aws_iam_role" "scheduler" {
  name = "${var.name}-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "scheduler.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "scheduler" {
  name = "${var.name}-scheduler-policy"
  role = aws_iam_role.scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = [aws_lambda_function.this.arn]
      }
    ]
  })
}

resource "aws_scheduler_schedule" "this" {
  name                         = "${var.name}-market-hours"
  group_name                   = "default"
  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = var.schedule_timezone
  state                        = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.this.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode(var.schedule_input)
  }
}

resource "aws_lambda_permission" "allow_scheduler" {
  statement_id  = "AllowExecutionFromEventBridgeScheduler"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.this.function_name
  principal     = "scheduler.amazonaws.com"
  source_arn    = aws_scheduler_schedule.this.arn
}
