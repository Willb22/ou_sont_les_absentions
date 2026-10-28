locals {
  git_branch = var.deploy_target == "prod" ? "main" : "dev"
  namespace  = var.deploy_target == "prod" ? "ousontlesabstentions.org" : "${var.deploy_target}.ousontlesabstentions.org"
  common_tags = {
    Project     = "ou_sont_les_absentions"
    Environment = var.deploy_target
    ManagedBy   = "Terraform"
  }
  name_prefix = "ouabst-${var.deploy_target}"
}
