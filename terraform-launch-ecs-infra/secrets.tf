data "aws_secretsmanager_secret" "db_password" {
  arn = "arn:aws:secretsmanager:eu-north-1:632279305326:secret:postgres/password-MA7JN9"
}

data "aws_secretsmanager_secret_version" "db_password" {
  secret_id = data.aws_secretsmanager_secret.db_password.id
}

locals {
  db_password_plain = try(jsondecode(data.aws_secretsmanager_secret_version.db_password.secret_string).password, data.aws_secretsmanager_secret_version.db_password.secret_string)
}
