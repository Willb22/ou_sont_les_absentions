#!/bin/bash
#set -e #Comment here to let script pursue after a failed line
CURRENT_DIR="/home/ubuntu/ou_sont_les_absentions/scripts_ec2_deployment"
set -a      # auto-export variables so are available to child processes like aws
source /home/ubuntu/ou_sont_les_absentions/.env
set +a      # avoid auto-export of future variable defined after this line

sh "$CURRENT_DIR/install_docker_docker_compose.sh"
sudo -E sh "$CURRENT_DIR/restore_backup_https_cert.sh"
sudo -E sh "$CURRENT_DIR/timer_clear_page_cache.sh"
sudo -E sh "$CURRENT_DIR/run_etl.sh"
sudo -E sh "$CURRENT_DIR/install_nginx.sh"
sudo -E sh "$CURRENT_DIR/certbot_ca_certificate.sh"
sudo -E sh "$CURRENT_DIR/run_webapp.sh"
