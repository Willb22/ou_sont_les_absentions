resource "aws_cloudwatch_log_group" "etl" {
  name              = "/ecs/${local.name_prefix}-etl"
  retention_in_days = 7

  tags = local.common_tags
}

resource "aws_cloudwatch_log_group" "web" {
  name              = "/ecs/${local.name_prefix}-web"
  retention_in_days = 7

  tags = local.common_tags
}