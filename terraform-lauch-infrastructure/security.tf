data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

resource "aws_security_group" "ssh_access" {
  name        = "allow-ssh"
  description = "Allow SSH from my public IP"

  ingress {
    description = "SSH from my IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["${chomp(data.http.my_ip.response_body)}/32"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

