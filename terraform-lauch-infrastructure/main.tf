


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
    Name = "${var.deploy_target}-ubuntu-22"
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

  #git clone -b ${local.git_branch} https://github.com/you/repo.git
  su - ubuntu -c "git clone --branch ${local.git_branch} https://github.com/Willb22/ou_sont_les_absentions.git /home/ubuntu/ou_sont_les_absentions"

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

  sudo -u ubuntu bash /home/ubuntu/ou_sont_les_absentions/scripts_ec2_deployment/bootstrap.sh
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
    Name = "${var.deploy_target}-terraform-web-eip"
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
