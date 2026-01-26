resource "aws_iam_role" "ec2_letsencrypt_role" {
  name = "ec2-letsencrypt-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "letsencrypt_s3_policy" {
  name = "letsencrypt-s3-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.letsencrypt.arn,
        "${aws_s3_bucket.letsencrypt.arn}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "attach_s3" {
  role       = aws_iam_role.ec2_letsencrypt_role.name
  policy_arn = aws_iam_policy.letsencrypt_s3_policy.arn
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "ec2-letsencrypt-profile"
  role = aws_iam_role.ec2_letsencrypt_role.name
}


