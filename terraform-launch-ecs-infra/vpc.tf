data "aws_vpc" "main" {
  id = "vpc-045621ccf81395749"
}

data "aws_internet_gateway" "main" {
  filter {
    name   = "attachment.vpc-id"
    values = [data.aws_vpc.main.id]
  }
}
