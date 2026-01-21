#!/bin/bash
CERT_DIR="/etc/letsencrypt"
set -a      # auto-export variables so are available to child processes like aws
source /home/ubuntu/ou_sont_les_absentions/.env
set +a      # avoid auto-export of future variable defined after this line

apt-get install -y awscli
if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
    echo "Bucket $BUCKET does not exist. Creating..."
    aws s3 mb "s3://$BUCKET"
else
    echo "Bucket $BUCKET exists."
fi
echo "=== Checking S3 for existing Let's Encrypt certs ==="
if aws s3 ls "$S3_OBJ" >/dev/null 2>&1; then
    echo "Found Let's Encrypt backup in S3. Restoring..."
    aws s3 cp "$S3_OBJ" /home/ubuntu/letsencrypt-backup.tar.gz
    sudo tar xzpf /home/ubuntu/letsencrypt-backup.tar.gz -C /etc/
else
    echo "No cert backup found on S3"
fi