data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

data "aws_vpc" "default" {
  default = true
}

resource "aws_security_group" "inbound_outbound_access" {
  name        = "open-ports-${var.deploy_target}"
  description = "Specify ingress and egress rules for EC2"
  #vpc_id = data.aws_vpc.default.id
  vpc_id = "vpc-045621ccf81395749"
  ingress {
    description = "SSH from my IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }


  # Dev env via port 5000
  ingress {
    description = "Web App Port 5000"
    from_port   = 5000
    to_port     = 5000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  # HTTP and HTTPS via port 80 and 443
  ingress {
  description = "HTTP Port 80"
  from_port   = 80
  to_port     = 80
  protocol    = "tcp"
  cidr_blocks = ["0.0.0.0/0"]
}


  ingress {
  description = "HTTPS Port 443"
  from_port   = 443
  to_port     = 443
  protocol    = "tcp"
  cidr_blocks = ["0.0.0.0/0"]
}



  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

