data "aws_ecr_repository" "etl" {
  name = var.ecr_etl_repo_name
}

