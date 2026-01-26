
locals {
  git_branch = var.deploy_target == "prod" ? "main" : "dev"

  #current_git_branch = trimspace(chomp(shell("git rev-parse --abbrev-ref HEAD")))


  namespace = (var.deploy_target == "prod"
    ? "stage.ousontlesabstentions.org"
    : "dev.ousontlesabstentions.org")

  nginx_conf = templatefile("${path.module}/nginx.conf.tpl", {
    namespace = local.namespace
  })
  cert_bucket = "ousontlesabstentions-letsencrypt-${var.deploy_target}-ec2"

  env_static = file("${path.module}/../.env")   # hardcoded secrets
  env_dynamic = templatefile("${path.module}/env_dynamic.tpl", {
    ENV = var.deploy_target
    APP_ENV=var.app_env
    NAMESPACE   = local.namespace
    GIT_BRANCH  = local.git_branch
    BUCKET = local.cert_bucket
  })
  env_file = "${trimspace(local.env_static)}\n${trimspace(local.env_dynamic)}\n"


}
