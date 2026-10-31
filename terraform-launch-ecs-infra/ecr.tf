data "aws_ecr_repository" "etl" {
  name = var.ecr_etl_repo_name
}

data "aws_ecr_repository" "web" {
  name = var.ecr_web_repo_name
}