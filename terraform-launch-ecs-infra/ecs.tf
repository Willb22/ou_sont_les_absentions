data "aws_ecs_cluster" "existing" {
  cluster_name = "arn:aws:ecs:eu-north-1:632279305326:cluster/ou_sont_les_absentions-datafeed-cluster"
}

# ETL Task Definition (one-off task)
resource "aws_ecs_task_definition" "etl" {
  family                   = "${local.name_prefix}-etl-task"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.ecs_task_cpu
  memory                   = var.ecs_task_memory
  execution_role_arn       = "arn:aws:iam::632279305326:role/ecsTaskExecutionRole"
  task_role_arn            = "arn:aws:iam::632279305326:role/ecsTaskExecutionRole"

  container_definitions = jsonencode([
    {
      name      = "etl-container"
      image     = "${data.aws_ecr_repository.etl.repository_url}:${var.etl_image_tag}"
      essential = true

      environment = [
        { name = "APP_ENV", value = var.app_env },
        { name = "TABLE_CONNECTION", value = "AWS" },
        { name = "RDS_DB_HOST", value = aws_db_instance.postgres.address },
        { name = "PORT_POSTGRESQL_AWS", value = "5432" },
        { name = "DB_NAME", value = var.db_name },
        { name = "DB_USER", value = var.db_username }
      ]

      secrets = [
        { name = "PASSPOSTGRES", valueFrom = local.db_password_secret_reference }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.etl.name
          awslogs-region        = "eu-north-1"
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])

  tags = local.common_tags
}
