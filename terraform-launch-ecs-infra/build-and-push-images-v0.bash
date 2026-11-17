#!/usr/bin/env bash
set -euo pipefail
# Use pwd to be certain the absolute path is returned
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd -- "$script_dir/.." && pwd)"

config_file="$script_dir/deployment-config.json"
image_tag="${1:-latest}"
web_repo="$(jq -er '.ecr.web_repository | select(type == "string" and length > 0)' "$config_file")"
etl_repo="$(jq -er '.ecr.etl_repository | select(type == "string" and length > 0)' "$config_file")"
aws_region="$(jq -er '.aws_region | select(type == "string" and length > 0)' "$config_file")"

terraform -chdir="$script_dir" init -input=false
terraform -chdir="$script_dir" apply \
  -input=false \
  -auto-approve \
  -target=aws_ecr_repository.web \
  -target=aws_ecr_repository.etl \
  -var="deploy_target=ecs" \
  -var="app_env=staging"

registry="632279305326.dkr.ecr.${aws_region}.amazonaws.com"
web_image="${registry}/${web_repo}:${image_tag}"
etl_image="${registry}/${etl_repo}:${image_tag}"

aws ecr get-login-password --region "$aws_region" |
  docker login --username AWS --password-stdin "$registry"

docker build --file "$project_root/web_app/Dockerfile" \
  --tag "$web_image" \
  "$project_root"
docker build --file "$project_root/process_data/Dockerfile" \
  --tag "$etl_image" \
  "$project_root"

docker push "$web_image"
docker push "$etl_image"

echo "Pushed web image: $web_image"
echo "Pushed ETL image: $etl_image"
