data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "nano" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "nano-ou-sont-les-abstentions"
  }
}

resource "aws_internet_gateway" "nano" {
  vpc_id = aws_vpc.nano.id

  tags = {
    Name = "nano-ou-sont-les-abstentions"
  }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.nano.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true

  tags = {
    Name = "nano-ou-sont-les-abstentions-public"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.nano.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.nano.id
  }

  tags = {
    Name = "nano-ou-sont-les-abstentions-public"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}
