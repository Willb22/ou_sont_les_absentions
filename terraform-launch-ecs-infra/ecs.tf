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


# Web Task Definition
resource "aws_ecs_task_definition" "web" {
  family                   = "${local.name_prefix}-web-task"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.ecs_task_cpu
  memory                   = var.ecs_task_memory
  execution_role_arn       = "arn:aws:iam::632279305326:role/ecsTaskExecutionRole"
  task_role_arn            = "arn:aws:iam::632279305326:role/ecsTaskExecutionRole"

  container_definitions = jsonencode([
    {
      name      = "web-container"
      image     = "${data.aws_ecr_repository.web.repository_url}:${var.web_image_tag}"
      essential = true
      portMappings = [
        {
          containerPort = var.container_port_web
          protocol      = "tcp"
        }
      ]

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
          awslogs-group         = aws_cloudwatch_log_group.web.name
          awslogs-region        = "eu-north-1"
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])

  tags = local.common_tags
}

# ECS Service for Web App
resource "aws_ecs_service" "web" {
  name            = "${local.name_prefix}-web-service"
  cluster         = data.aws_ecs_cluster.existing.id
  task_definition = aws_ecs_task_definition.web.arn
  desired_count   = 1 #If a task exits or fails health checks, ECS will often launch a replacement to maintain this value
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = local.public_subnet_ids
    security_groups  = [data.aws_security_group.ecs_web_existing.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.web.arn
    container_name   = "web-container"
    container_port   = var.container_port_web
  }

  depends_on = [aws_lb_listener.https, aws_lb_listener.http]

  tags = local.common_tags
}
