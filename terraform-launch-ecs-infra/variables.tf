variable "deploy_target" {
  type        = string
  description = "Deployment target: ecs or prod"
  validation {
    condition     = contains(["ecs", "prod"], var.deploy_target)
    error_message = "deploy_target must be ecs or prod"
  }
}

variable "app_env" {
  type        = string
  description = "APP_ENV value (dev/staging/prod)"
}

variable "rds_instance_class" {
  type        = string
  default     = "db.t3.micro"
  description = "RDS instance class"
}

variable "rds_allocated_storage" {
  type        = number
  default     = 20
  description = "RDS allocated storage (GB)"
}

variable "db_name" {
  type        = string
  default     = "ousontlesabstentions"
  description = "RDS database name"
}

variable "db_username" {
  type        = string
  default     = "postgres"
  description = "RDS master username"
}

variable "etl_image_tag" {
  type        = string
  default     = "latest"
  description = "Tag of the ETL image in ECR"
}

variable "ecs_task_memory" {
  type        = string
  default     = "512"
  description = "Fargate memory (MiB)"
}

variable "ecs_task_cpu" {
  type        = string
  default     = "256"
  description = "Fargate CPU units"
}

variable "ecr_etl_repo_name" {
  type        = string
  default     = "ou_sont_les_absentions-datafeed"
  description = "ECR repo name for ETL image"
}

variable "ecr_web_repo_name" {
  type        = string
  default     = "ou_sont_les_absentions-webapp"
  description = "ECR repo name for web app image"
}

variable "domain_name" {
  type        = string
  default     = "ousontlesabstentions.org"
  description = "Root domain name"
}

variable "web_image_tag" {
  type        = string
  default     = "latest"
  description = "Tag of the web image in ECR"
}

variable "container_port_web" {
  type        = number
  default     = 5000
  description = "Port the web app container listens on"
}