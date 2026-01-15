


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
# Specify Security group for all ports on EC2.
 vpc_security_group_ids = [
   aws_security_group.inbound_outbound_access.id
 ]

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
  #set -e #Comment here to let script pursue after a failed line


  ${file("${path.module}/install_docker_docker_compose.sh")}
  ${file("${path.module}/git_clone_project.sh")}

  cat << 'ENVEOF' > /home/ubuntu/ou_sont_les_absentions/.env
  ${file("../.env")}
  ENVEOF
  
  # Run docker compose
  ${file("${path.module}/run_docker_compose.sh")}
  # Prepare web environment
  ${file("${path.module}/install_nginx.sh")}
  
  sudo cat << 'NGINXEOF' > /etc/nginx/nginx.conf
  ${file("nginx_dev_conf.txt")}
  NGINXEOF

  EOF
# String interpolation

}

# -----------------------------
# Elastic IP
# -----------------------------
resource "aws_eip" "web_eip" {
  #instance = aws_instance.ubuntu_ec2.id # Legacy Terraform feature
  domain   = "vpc"

  tags = {
    Name = "terraform-web-eip"
  }
  lifecycle {
  prevent_destroy = true
  }
}
# Associate EIP to the EC2 instance
resource "aws_eip_association" "web_eip" {
  instance_id   = aws_instance.ubuntu_ec2.id
  allocation_id = aws_eip.web_eip.id
}

