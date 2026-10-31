# Use existing security groups from AWS Console
data "aws_security_group" "alb_existing" {
  id = "sg-08a4454e56567ed2b"
}

data "aws_security_group" "ecs_etl_existing" {
  id = "sg-0e73fc5f4f2b042ea"
}

data "aws_security_group" "ecs_web_existing" {
  id = "sg-0b4f1fbf67d6cce87"
}

data "aws_security_group" "rds_existing" {
  id = "sg-09b25a4c721301281"
}

