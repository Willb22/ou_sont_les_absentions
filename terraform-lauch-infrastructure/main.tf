


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
  iam_instance_profile = aws_iam_instance_profile.ec2_profile.name
  vpc_security_group_ids = [
    aws_security_group.inbound_outbound_access.id
  ]

  # AWS key pair for ssh and scp
  key_name = "first_ec2"

  tags = {
    Name = "dev"
  }
  #associate_public_ip_address = false # Disable ephemeral public IPs explicitly

  root_block_device {
    volume_size = 35       # <-- 35 GB
    volume_type = "gp3"    # recommended
  }

  user_data = <<-EOF
  #!/bin/bash
  #set -e #Comment here to let script pursue after a failed line
  apt-get update -y

  ${file("${path.module}/install_docker_docker_compose.sh")}
  git clone -b ${local.git_branch} https://github.com/you/repo.git
  #${file("${path.module}/git_clone_project.sh")}
  # cat << 'ENVEOF' > /home/ubuntu/ou_sont_les_absentions/.env
  # ${file("../.env")}
  # ENVEOF

  cat << 'ENVEOF' > /home/ubuntu/ou_sont_les_absentions/.env
  ${local.env_file}
  ENVEOF

  # export all variables for the current shell session
  export $(grep -v '^#' /home/ubuntu/ou_sont_les_absentions/.env | xargs)

  # persist environment for all login shells
  cat << 'PROFILEEOF' > /etc/profile.d/myapp_env.sh
  export $(grep -v '^#' /home/ubuntu/ou_sont_les_absentions/.env | xargs)
  PROFILEEOF
  chmod +x /etc/profile.d/myapp_env.sh

  ${file("${path.module}/restore_backup_https_cert.sh")}
  ${file("${path.module}/timer_clear_page_cache.sh")}

  # Run docker compose
  ${file("${path.module}/run_etl.sh")}
  # Prepare web environment
  ${file("${path.module}/install_nginx.sh")}

  # sudo cat << 'NGINXEOF' > /etc/nginx/nginx.conf
  # ${file("nginx_dev_conf.txt")}
  # NGINXEOF

  sudo cat << 'NGINXEOF' > /etc/nginx/nginx.conf
  ${local.nginx_conf}
  NGINXEOF
  ${file("${path.module}/certbot_ca_certificate.sh")}



  ${file("${path.module}/run_webapp.sh")}
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
# Prevent accidental prod deploy from non-main branch
# resource "null_resource" "check_branch" {
#   count = local.current_git_branch != "main" && var.deploy_target == "prod" ? 1 : 0
#
#   provisioner "local-exec" {
#     command = "echo 'ERROR: Attempted prod deploy from non-main branch!' && exit 1"
#   }
# }

