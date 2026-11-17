#!/usr/bin/env bash
#Allow exit code of entire pipeline to be 1 if any command exit code 1
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
AWS_REGION="$(jq -er '.aws_region | select(type == "string" and length > 0)' "$SCRIPT_DIR/deployment-config.json")"

# 2. Retrieve infrastructure details
CLUSTER=$(terraform -chdir="$SCRIPT_DIR" output -raw ecs_cluster_name)
TASK_DEFINITION=$(terraform -chdir="$SCRIPT_DIR" output -raw etl_task_definition_arn)
SUBNETS=$(terraform -chdir="$SCRIPT_DIR" output -json ecs_etl_subnet_ids)
SECURITY_GROUP=$(terraform -chdir="$SCRIPT_DIR" output -raw ecs_etl_security_group_id)

echo "Starting ETL task..."

# 3. Configure networking
NETWORK_CONFIG=$(jq -n \
  --argjson subnets "$SUBNETS" \
  --arg sg "$SECURITY_GROUP" \
  '{
    awsvpcConfiguration: {
      subnets: $subnets,
      securityGroups: [$sg],
      assignPublicIp: "ENABLED"
    }
  }')

# 4. Start the ECS task
RUN_RESULT=$(aws ecs run-task \
  --region "$AWS_REGION" \
  --cluster "$CLUSTER" \
  --task-definition "$TASK_DEFINITION" \
  --launch-type FARGATE \
  --network-configuration "$NETWORK_CONFIG")

