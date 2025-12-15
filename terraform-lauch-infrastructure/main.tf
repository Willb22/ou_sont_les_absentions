


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

}