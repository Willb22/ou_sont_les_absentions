resource "aws_db_instance" "postgres" {
  identifier             = "${local.name_prefix}-postgres"
  engine                 = "postgres"
  engine_version         = "14.18"
  instance_class         = var.rds_instance_class
  allocated_storage      = var.rds_allocated_storage
  storage_type           = "gp3"
  db_name                = var.db_name
  username               = var.db_username
  password               = local.db_password_plain
  port                   = 5432
  db_subnet_group_name   = "default"
  vpc_security_group_ids = [data.aws_security_group.rds_existing.id]
  publicly_accessible    = false
  skip_final_snapshot    = true
  deletion_protection    = false
  multi_az               = false

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-rds"
  })
}
