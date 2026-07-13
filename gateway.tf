provider "aws" {
  region  = "eu-central-1"
  profile = "michael"
}

variable "ray_head_public_ip" {
  type        = string
  description = "Public IP address of the Ray head instance"
  default     = "127.0.0.1"
}

# =============================================================================
# 1. Dynamic Resource Discovery (VPC, Subnets, EC2 Head, Route 53 Zone)
# =============================================================================

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_instance" "head" {
  filter {
    name   = "ip-address"
    values = [var.ray_head_public_ip]
  }
}

data "aws_route53_zone" "primary" {
  name = "health.odyn.network."
}

# =============================================================================
# 2. Private VPC Integration (VPC Link & Network Load Balancer)
# =============================================================================

resource "aws_lb" "serve_nlb" {
  name               = "ray-serve-nlb"
  internal           = true
  load_balancer_type = "network"
  subnets            = data.aws_subnets.default.ids
}

resource "aws_lb_target_group" "serve_tg" {
  name        = "ray-serve-tg"
  port        = 80
  protocol    = "TCP"
  vpc_id      = data.aws_vpc.default.id
  target_type = "instance"

  health_check {
    port     = "80"
    protocol = "TCP"
  }
}

resource "aws_lb_target_group_attachment" "serve_tg_attach" {
  target_group_arn = aws_lb_target_group.serve_tg.arn
  target_id        = data.aws_instance.head.id
  port             = 80
}

resource "aws_lb_listener" "serve_listener" {
  load_balancer_arn = aws_lb.serve_nlb.arn
  port              = 80
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.serve_tg.arn
  }
}

resource "aws_api_gateway_vpc_link" "vpc_link" {
  name        = "ray-serve-vpc-link"
  target_arns = [aws_lb.serve_nlb.arn]
}

# =============================================================================
# 3. REST API & Versioned Surface (/v1)
# =============================================================================

resource "aws_api_gateway_rest_api" "api" {
  name        = "odyn-oem-api"
  description = "Secure API Gateway for Heterogeneous Cross-OEM Cluster"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_resource" "v1" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  parent_id   = aws_api_gateway_rest_api.api.root_resource_id
  path_part   = "v1"
}

resource "aws_api_gateway_resource" "proxy" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  parent_id   = aws_api_gateway_resource.v1.id
  path_part   = "{proxy+}"
}

resource "aws_api_gateway_method" "any" {
  rest_api_id      = aws_api_gateway_rest_api.api.id
  resource_id      = aws_api_gateway_resource.proxy.id
  http_method      = "ANY"
  authorization    = "NONE"
  api_key_required = true

  request_parameters = {
    "method.request.path.proxy" = true
  }
}

resource "aws_api_gateway_integration" "integration" {
  rest_api_id             = aws_api_gateway_rest_api.api.id
  resource_id             = aws_api_gateway_resource.proxy.id
  http_method             = aws_api_gateway_method.any.http_method
  type                    = "HTTP_PROXY"
  integration_http_method = "ANY"
  uri                     = "http://$${stageVariables.vpc_link_dns}/v1/{proxy}"
  connection_type         = "VPC_LINK"
  connection_id           = aws_api_gateway_vpc_link.vpc_link.id

  request_parameters = {
    "integration.request.path.proxy" = "method.request.path.proxy"
  }
}

# =============================================================================
# 4. Stages, Deployment, and Access Logging
# =============================================================================

resource "aws_iam_role" "apigw_cw" {
  name = "api-gateway-cloudwatch-global"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "apigateway.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "apigw_cw_attach" {
  role       = aws_iam_role.apigw_cw.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonAPIGatewayPushToCloudWatchLogs"
}

resource "aws_api_gateway_account" "global" {
  cloudwatch_role_arn = aws_iam_role.apigw_cw.arn
}

resource "aws_cloudwatch_log_group" "api_logs" {
  name              = "API-Gateway-Access-Logs-odyn"
  retention_in_days = 30
}

resource "aws_api_gateway_deployment" "deployment" {
  depends_on  = [aws_api_gateway_integration.integration]
  rest_api_id = aws_api_gateway_rest_api.api.id

  triggers = {
    redeployment = sha256(jsonencode([
      aws_api_gateway_resource.v1.id,
      aws_api_gateway_resource.proxy.id,
      aws_api_gateway_method.any.id,
      aws_api_gateway_integration.integration.id,
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "prod" {
  deployment_id = aws_api_gateway_deployment.deployment.id
  rest_api_id   = aws_api_gateway_rest_api.api.id
  stage_name    = "prod"

  variables = {
    vpc_link_dns = aws_lb.serve_nlb.dns_name
  }

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_logs.arn
    format          = "{\"requestId\":\"$context.requestId\",\"apiKeyId\":\"$context.identity.apiKeyId\",\"ip\":\"$context.identity.sourceIp\",\"caller\":\"$context.identity.caller\",\"user\":\"$context.identity.user\",\"requestTime\":\"$context.requestTime\",\"httpMethod\":\"$context.httpMethod\",\"resourcePath\":\"$context.resourcePath\",\"status\":\"$context.status\",\"protocol\":\"$context.protocol\",\"responseLength\":\"$context.responseLength\",\"latency\":\"$context.responseLatency\"}"
  }
}

# =============================================================================
# 5. Custom Domain Name, SSL (ACM), and DNS Routing (Route 53)
# =============================================================================

# Custom Domain and DNS validation are disabled since health.odyn.network is not publicly delegated under the parent domain registrar.
# Users can route via the native regional API Gateway execute-api URL.

# =============================================================================
# 6. Auth, Usage Plan, and Rate Limiting
# =============================================================================

resource "aws_api_gateway_usage_plan" "plan" {
  name = "standard-usage-plan"

  api_stages {
    api_id = aws_api_gateway_rest_api.api.id
    stage  = aws_api_gateway_stage.prod.stage_name
  }

  throttle_settings {
    burst_limit = 200
    rate_limit  = 100
  }

  quota_settings {
    limit  = 10000
    period = "DAY"
  }
}

resource "aws_api_gateway_api_key" "consumer_key" {
  name    = "external-partner-key"
  enabled = true
}

resource "aws_api_gateway_usage_plan_key" "main" {
  key_id        = aws_api_gateway_api_key.consumer_key.id
  key_type      = "API_KEY"
  usage_plan_id = aws_api_gateway_usage_plan.plan.id
}

# =============================================================================
# 7. Observability CloudWatch Dashboard
# =============================================================================

resource "aws_cloudwatch_dashboard" "api_dashboard" {
  dashboard_name = "API-Gateway-Performance"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/ApiGateway", "Count", "ApiName", "odyn-oem-api", { stat = "Sum" }],
            ["AWS/ApiGateway", "4XXError", "ApiName", "odyn-oem-api", { stat = "Sum", color = "#ff7f0e" }],
            ["AWS/ApiGateway", "5XXError", "ApiName", "odyn-oem-api", { stat = "Sum", color = "#d62728" }]
          ]
          period  = 60
          region  = "eu-central-1"
          title   = "Request Traffic & Error Count"
          view    = "timeSeries"
          stacked = false
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/ApiGateway", "Latency", "ApiName", "odyn-oem-api", { stat = "p99", label = "p99 Latency (ms)", color = "#2ca02c" }],
            ["AWS/ApiGateway", "Latency", "ApiName", "odyn-oem-api", { stat = "Average", label = "Avg Latency (ms)" }]
          ]
          period = 60
          region = "eu-central-1"
          title  = "API Gateway Latency Metrics"
          view   = "timeSeries"
        }
      }
    ]
  })
}

# =============================================================================
# 8. S3 Buckets for Inference Outputs & Logs (Encrypted, Versioned, Block Public)
# =============================================================================

resource "aws_s3_bucket" "logs" {
  bucket        = "odyn-logs-145689194487"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "logs_versioning" {
  bucket = aws_s3_bucket.logs.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs_encryption" {
  bucket = aws_s3_bucket.logs.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "logs_public_block" {
  bucket                  = aws_s3_bucket.logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "outputs" {
  bucket        = "odyn-inference-outputs-145689194487"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "outputs_versioning" {
  bucket = aws_s3_bucket.outputs.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "outputs_encryption" {
  bucket = aws_s3_bucket.outputs.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "outputs_public_block" {
  bucket                  = aws_s3_bucket.outputs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_logging" "outputs_logging" {
  bucket        = aws_s3_bucket.outputs.id
  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "s3_access_logs/outputs/"
}
