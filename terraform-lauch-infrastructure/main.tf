


# Get Ubuntu 22
data "aws_ami" "ubuntu_22" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}


resource "aws_instance" "ubuntu_ec2" {
  ami           = data.aws_ami.ubuntu_22.id
  instance_type = "t3.micro"

  # AWS key pair for ssh and scp
  key_name = "first_ec2"

  tags = {
    Name = "Ubuntu22"
  }

  root_block_device {
    volume_size = 35       # <-- 35 GB
    volume_type = "gp3"    # recommended
  }

  user_data = <<-EOF
  #!/bin/bash
  set -e

  ${file("${path.module}/install_docker_docker_compose.sh")}
  ${file("${path.module}/git_clone_project.sh")}
  EOF



}

# -----------------------------
# Elastic IP
# -----------------------------
resource "aws_eip" "web_eip" {
  instance = aws_instance.ubuntu_ec2.id
  domain   = "vpc"

  tags = {
    Name = "terraform-web-eip"
  }
}

