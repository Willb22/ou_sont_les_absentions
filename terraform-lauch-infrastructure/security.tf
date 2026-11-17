data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

resource "aws_security_group" "inbound_outbound_access" {
  name        = "open-ports-${var.deploy_target}"
  description = "Specify ingress and egress rules for EC2"
  vpc_id      = aws_vpc.main.id
  ingress {
    description = "SSH from my IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["${chomp(data.http.my_ip.response_body)}/32"]
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

