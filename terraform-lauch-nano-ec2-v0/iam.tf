resource "aws_iam_role" "builder" {
  name = "nano-ou-sont-les-abstentions-builder"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "builder" {
  name = "nano-ou-sont-les-abstentions-builder"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer",
          "ecr:CompleteLayerUpload",
          "ecr:InitiateLayerUpload",
          "ecr:PutImage",
          "ecr:UploadLayerPart"
        ]
        Resource = [
          aws_ecr_repository.datafeed.arn,
          aws_ecr_repository.webapp.arn
        ]
      }

    ]
  })
}

resource "aws_iam_role_policy_attachment" "builder" {
  role       = aws_iam_role.builder.name
  policy_arn = aws_iam_policy.builder.arn
}


resource "aws_iam_instance_profile" "builder" {
  name = "nano-ou-sont-les-abstentions-builder"
  role = aws_iam_role.builder.name
}



resource "aws_iam_role" "runner" {
  name = "nano-ou-sont-les-abstentions-runner"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "runner" {
  name = "nano-ou-sont-les-abstentions-runner"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer"
        ]
        Resource = [
          aws_ecr_repository.webapp.arn
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = var.postgres_secret_arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "runner" {
  role       = aws_iam_role.runner.name
  policy_arn = aws_iam_policy.runner.arn
}

resource "aws_iam_instance_profile" "runner" {
  name = "nano-ou-sont-les-abstentions-runner"
  role = aws_iam_role.runner.name
}
