#!/bin/bash
#set -e #Comment here to let script pursue after a failed line
CURRENT_DIR="/home/ubuntu/ou_sont_les_absentions/scripts_ec2_deployment"
# Ensure env is loaded
export $(grep -v '^#' /home/ubuntu/ou_sont_les_absentions/.env | xargs)

sh "$CURRENT_DIR/install_docker_docker_compose.sh"
sudo sh "$CURRENT_DIR/restore_backup_https_cert.sh"
sudo sh "$CURRENT_DIR/timer_clear_page_cache.sh"
sudo sh "$CURRENT_DIR/run_etl.sh"
sudo sh "$CURRENT_DIR/install_nginx.sh"
sudo sh "$CURRENT_DIR/certbot_ca_certificate.sh"
sudo sh "$CURRENT_DIR/run_webapp.sh"
