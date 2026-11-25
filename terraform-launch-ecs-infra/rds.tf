resource "aws_db_instance" "postgres" {
  identifier             = "${local.name_prefix}-postgres"
  engine                 = "postgres"
  engine_version         = "18.3"
  instance_class         = var.rds_instance_class
  allocated_storage      = var.rds_allocated_storage
  storage_type           = "gp3"
  db_name                = var.db_name
  username               = var.db_username
  port                   = 5431
  db_subnet_group_name   = "default"
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false
  skip_final_snapshot    = true
  deletion_protection    = false
  multi_az               = false
  manage_master_user_password = true

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-rds"
  })
}
