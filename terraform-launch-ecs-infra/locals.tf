locals {
  deployment_config = jsondecode(file("${path.module}/deployment-config.json"))
  aws_region        = local.deployment_config.aws_region
  ecr_web_repo_name = local.deployment_config.ecr.web_repository
  ecr_etl_repo_name = local.deployment_config.ecr.etl_repository
  git_branch        = var.deploy_target == "prod" ? "main" : "dev"
  namespace         = var.deploy_target == "prod" ? "ousontlesabstentions.org" : "${var.deploy_target}.ousontlesabstentions.org"
  public_subnet_ids = sort(tolist(data.aws_subnets.public.ids))
  common_tags = {
    Project     = "ou_sont_les_absentions"
    Environment = var.deploy_target
    ManagedBy   = "Terraform"
  }
  name_prefix = "ouabst-${var.deploy_target}"
}
