resource "aws_s3_bucket" "letsencrypt" {
  bucket = var.letsencrypt_bucket_name
}

resource "aws_s3_bucket_versioning" "letsencrypt" {
  bucket = aws_s3_bucket.letsencrypt.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "letsencrypt" {
  bucket = aws_s3_bucket.letsencrypt.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "letsencrypt" {
  bucket = aws_s3_bucket.letsencrypt.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
# letsencrypt_bucket_name variable declared here and defined in tfvars file
variable "letsencrypt_bucket_name" {
  description = "S3 bucket storing letsencrypt certs"
  type        = string
  #default     = "ousontlesabstentions-letsencrypt-dev-ec2"

}

