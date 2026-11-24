variable "aws_region" {
  description = "AWS region for the ECR repositories and EC2 instances."
  type        = string
  default     = "eu-west-3"
}

variable "git_repository" {
  description = "HTTPS URL of the application repository."
  type        = string
  default     = "https://github.com/Willb22/ou_sont_les_absentions.git"
}

variable "git_branch" {
  description = "Branch cloned by the builder and runner."
  type        = string
  default     = "dev"
}
# variable "image_build_token" {
#   description = "Change this value to rebuild and redeploy images from the same branch."
#   type        = string
#   default     = "initial"
# }

variable "build_status_parameter" {
  description = "ec2 runner user_data has a aws ssm get-parameter --name BUILD_STATUS_NAME to check if status is complete, pending or failed"
  type        = string
  default     = "/nano-ou-sont-les-abstentions/build-status/current"
}
variable "app_env" {
  description = "Application configuration name from configs/<name>_config.yaml."
  type        = string
  default     = "staging"
}

variable "database_name" {
  description = "PostgreSQL database created by the PostgreSQL container."
  type        = string
  default     = "dev_ou_sont_les_abstentions"
}
variable "builder_table_connection" {
  description = "Database connection mode used by the builder."
  type        = string
  default     = "EC2_REMOTE"
}
variable "database_port" {
  description = "Port for connections from web app or etl to postgresql db."
  type        = number
  default     = 5431
}

variable "postgres_username" {
  description = "PostgreSQL username."
  type        = string
  default     = "postgres"
}
variable "postgres_secret_arn" {
  description = "ARN of a Secrets Manager secret whose entire SecretString is the PostgreSQL password."
  type        = string
}
variable "root_volume_size_gb" {
  description = "EC2 root volume size in GiB."
  type        = number
  default     = 20
}