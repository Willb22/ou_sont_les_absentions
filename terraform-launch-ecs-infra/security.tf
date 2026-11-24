resource "aws_security_group" "alb" {
  name        = "alb-ousontlesabstentions-1"
  description = "Allows inbound traffic to ports 80 and 443 for http and https"
  vpc_id      = data.aws_vpc.main.id
  egress      = []

  tags = merge(local.common_tags, {
    Name = "alb-ousontlesabstentions-1"
  })
}

resource "aws_vpc_security_group_ingress_rule" "alb_http" {
  security_group_id = aws_security_group.alb.id
  description       = "Allow HTTP"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 80
  to_port           = 80
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "alb_https" {
  security_group_id = aws_security_group.alb.id
  description       = "Allow HTTPS"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  to_port           = 443
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "alb_to_web" {
  security_group_id            = aws_security_group.alb.id
  description                  = "Allow traffic from ports 443 and 80 to redirect to port 5000 where flask is"
  referenced_security_group_id = aws_security_group.ecs_web.id
  from_port                    = 5000
  to_port                      = 5000
  ip_protocol                  = "tcp"
}

resource "aws_security_group" "ecs_etl" {
  name        = "ecs-etl-ousontlesabstentions-1"
  description = "Allow outbound from etl container. Name of this security group is then referenced inside the security group for RDS DB"
  vpc_id      = data.aws_vpc.main.id
  egress      = []

  tags = merge(local.common_tags, {
    Name = "ecs-etl-ousontlesabstentions-1"
  })
}

resource "aws_vpc_security_group_egress_rule" "ecs_etl_all" {
  security_group_id = aws_security_group.ecs_etl.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_security_group" "ecs_web" {
  name        = "ecs-webapp-ousontlesabstentions-1"
  description = "Allow outbound from container. Name of this security group is then referenced inside the security group for RDS DB"
  vpc_id      = data.aws_vpc.main.id
  egress      = []

  tags = merge(local.common_tags, {
    Name = "ecs-webapp-ousontlesabstentions-1"
  })
}

resource "aws_vpc_security_group_ingress_rule" "ecs_web_from_alb" {
  security_group_id            = aws_security_group.ecs_web.id
  description                  = "Allow load balander to direct traffic towards port 5000 where Flask is"
  referenced_security_group_id = aws_security_group.alb.id
  from_port                    = 5000
  to_port                      = 5000
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "ecs_web_all" {
  security_group_id = aws_security_group.ecs_web.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_security_group" "rds" {
  name        = "rds-db-ousontlesabstentions-1"
  description = "Allow TCP access from ETL container/task on ECS to DB on RDS"
  vpc_id      = data.aws_vpc.main.id
  egress      = []

  tags = merge(local.common_tags, {
    Name = "rds-db-ousontlesabstentions-1"
  })
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_ecs_etl" {
  security_group_id            = aws_security_group.rds.id
  description                  = "Allow connection from ETL ecs task"
  referenced_security_group_id = aws_security_group.ecs_etl.id
  from_port                    = aws_db_instance.postgres.port
  to_port                      = aws_db_instance.postgres.port
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_ecs_web" {
  security_group_id            = aws_security_group.rds.id
  description                  = "Allow connection from webapp ecs task"
  referenced_security_group_id = aws_security_group.ecs_web.id
  from_port                    = aws_db_instance.postgres.port
  to_port                      = aws_db_instance.postgres.port
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "rds_all" {
  security_group_id = aws_security_group.rds.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}
