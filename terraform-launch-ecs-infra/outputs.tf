output "vpc_id" {
  description = "VPC ID"
  value       = data.aws_vpc.main.id
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint"
  value       = aws_db_instance.postgres.address
}

output "ecr_etl_repository_url" {
  description = "ECR repository URL for ETL"
  value       = data.aws_ecr_repository.etl.repository_url
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = data.aws_ecs_cluster.existing.cluster_name
}

output "ecs_etl_security_group_id" {
  description = "Security group to use when launching the ETL task"
  value       = data.aws_security_group.ecs_etl_existing.id
}

output "ecs_etl_subnet_ids" {
  description = "Public subnets to use when launching the ETL Fargate task"
  value = [
    "subnet-09337f20c0b553a0c",
    "subnet-09a7316f06b8db6e6",
    "subnet-00edd99aabade28b1",
  ]
}

output "etl_task_definition_arn" {
  description = "ETL task definition ARN"
  value       = aws_ecs_task_definition.etl.arn
}
