#!/bin/bash
#set -e #Comment here to let script pursue after a failed line

APP_DIR="/home/ubuntu/ou_sont_les_absentions"

# Wait for Docker daemon
until systemctl is-active --quiet docker; do
  sleep 1
done

# Run docker compose as user ubuntu
sudo -u ubuntu docker-compose \
  --project-directory "$APP_DIR" \
  up -d db
# Optional: wait for DB health
# docker-compose wait db  (Compose v2 only)

# Run sequential jobs (foreground = blocking)
sudo -u ubuntu docker-compose \
  --project-directory "$APP_DIR" \
  -f "$APP_DIR/docker-compose.yml" \
  -f "$APP_DIR/docker-compose.cloudinfra.yml" \
  run --name datafeed_extract datafeed --stage extract

sudo -u ubuntu docker-compose \
  --project-directory "$APP_DIR" \
  -f "$APP_DIR/docker-compose.yml" \
  -f "$APP_DIR/docker-compose.cloudinfra.yml" \
  run --name datafeed_france2017 datafeed --stage france2017

sudo -u ubuntu docker-compose \
  --project-directory "$APP_DIR" \
  -f "$APP_DIR/docker-compose.yml" \
  -f "$APP_DIR/docker-compose.cloudinfra.yml" \
  run --name datafeed_france2022 datafeed --stage france2022