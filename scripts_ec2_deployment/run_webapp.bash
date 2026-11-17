#!/bin/bash

APP_DIR="/home/ubuntu/ou_sont_les_absentions"

# Wait for Docker daemon
until systemctl is-active --quiet docker; do
  sleep 1
done

# Run docker compose as user ubuntu
sudo -u ubuntu docker-compose \
  --project-directory "$APP_DIR" \
  up -d webapp
