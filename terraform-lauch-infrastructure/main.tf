


# Get latest Ubuntu 24.04 LTS (Noble) AMI from Canonical
data "aws_ami" "ubuntu_24_04" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
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
  ami           = data.aws_ami.ubuntu_24_04.id
  instance_type = "t3.micro"

  # AWS key pair for ssh and scp
  key_name = "first_ec2"

  tags = {
    Name = "Ubuntu24-EC2"
  }
}