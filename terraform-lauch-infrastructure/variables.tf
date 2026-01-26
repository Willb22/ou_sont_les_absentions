
variable "deploy_target" {
  type        = string
  description = "Deployment target: dev or prod"
  validation {
    condition     = contains(["dev", "prod"], var.deploy_target)
    error_message = "deploy_target must be dev or prod"
  }
}

variable "app_env" {
  type = string
}

