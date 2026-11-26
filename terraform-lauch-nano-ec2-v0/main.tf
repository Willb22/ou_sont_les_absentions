resource "aws_ecr_repository" "webapp" {
  name                 = "nano-ou_sont_les_absentions-webapp"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }
  force_delete = true
}


resource "aws_ecr_repository" "datafeed" {
  name                 = "nano-ou_sont_les_absentions-datafeed"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }
  force_delete = true
}

data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}
data "aws_ami" "ubuntu_amd64" {
  most_recent = true
  owners      = ["099720109477"]

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

data "aws_ami" "ubuntu_arm64" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-arm64-server-*"]
  }

  filter {
    name   = "architecture"
    values = ["arm64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_security_group" "builder" {
  name        = "nano-ou-sont-les-abstentions-builder"
  description = "Outbound access for ECR image builds and ETL processing."
  vpc_id      = aws_vpc.nano.id


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


resource "aws_security_group" "runner" {
  name        = "nano-ou-sont-les-abstentions-runner"
  description = "Web access to the application runner."
  vpc_id      = aws_vpc.nano.id


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


  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}


resource "aws_instance" "builder" {
  #count                       = var.builder_enabled ? 1 : 0
  ami                         = data.aws_ami.ubuntu_amd64.id
  instance_type               = "t3.micro"
  subnet_id                   = aws_subnet.public.id
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.builder.id]
  iam_instance_profile        = aws_iam_instance_profile.builder.name
  user_data_replace_on_change = true
  # AWS key pair for ssh and scp
  key_name = "ssh_key_eu_west_3"

  user_data = templatefile("${path.module}/builder-user-data-V0.sh.tftpl", {
    aws_region             = var.aws_region
    git_repository         = var.git_repository
    git_branch             = var.git_branch
    ecr_datafeed_url       = aws_ecr_repository.datafeed.repository_url
    ecr_webapp_url         = aws_ecr_repository.webapp.repository_url
    app_env                = var.app_env
    database_name          = var.database_name
    table_connection       = var.builder_table_connection
    postgres_username      = var.postgres_username
    postgres_secret_arn    = var.postgres_secret_arn
  })

  root_block_device {
    volume_size = var.root_volume_size_gb
    volume_type = "gp3"
  }

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  tags = {
    Name = "nano-ou-sont-les-abstentions-etl-image-builder"
    Role = "etl-image-builder"
  }
}


resource "aws_instance" "runner" {
  ami                         = data.aws_ami.ubuntu_arm64.id
  instance_type               = "t4g.nano"
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.runner.id]
  iam_instance_profile        = aws_iam_instance_profile.runner.name
  user_data_replace_on_change = true
  # AWS key pair for ssh and scp
  key_name = "ssh_key_eu_west_3"

  user_data = templatefile("${path.module}/runner-user-data-V0.sh.tftpl", {
    aws_region             = var.aws_region
    app_env                = var.app_env
    database_name          = var.database_name
    table_connection       = var.table_connection
    database_host          = var.app_database_host
    database_port          = var.database_port
    postgres_secret_arn    = var.postgres_secret_arn
    git_repository         = var.git_repository
    git_branch             = var.git_branch
  })

  root_block_device {
    volume_size = var.root_volume_size_gb
    volume_type = "gp3"
  }

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  tags = {
    Name = "nano-ou-sont-les-abstentions-app-runner"
    Role = "app-runner"
  }
}