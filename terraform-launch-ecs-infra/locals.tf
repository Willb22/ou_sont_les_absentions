locals {
  deployment_config = jsondecode(file("${path.module}/deployment-config.json"))
  ecr_web_repo_name = local.deployment_config.ecr.web_repository
  ecr_etl_repo_name = local.deployment_config.ecr.etl_repository
  git_branch        = var.deploy_target == "prod" ? "main" : "dev"
  namespace         = var.deploy_target == "prod" ? "ousontlesabstentions.org" : "${var.deploy_target}.ousontlesabstentions.org"
  public_subnet_ids = [
    "subnet-09337f20c0b553a0c",
    "subnet-09a7316f06b8db6e6",
    "subnet-00edd99aabade28b1",
  ]
  common_tags = {
    Project     = "ou_sont_les_absentions"
    Environment = var.deploy_target
    ManagedBy   = "Terraform"
  }
  name_prefix = "ouabst-${var.deploy_target}"
}
