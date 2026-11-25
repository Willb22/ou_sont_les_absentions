locals {
  db_password_secret_reference = "${aws_db_instance.postgres.master_user_secret[0].secret_arn}:password::"
}
