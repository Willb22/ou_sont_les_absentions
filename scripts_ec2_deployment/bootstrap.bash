#!/bin/bash
#set -e #Comment here to let script pursue after a failed line
CURRENT_DIR="/home/ubuntu/ou_sont_les_absentions/scripts_ec2_deployment"
set -a      # auto-export variables so are available to child processes like aws
source /home/ubuntu/ou_sont_les_absentions/.env
set +a      # avoid auto-export of future variable defined after this line

bash "$CURRENT_DIR/install_docker_docker_compose.bash"
sudo -E bash "$CURRENT_DIR/restore_backup_https_cert.bash"
sudo -E bash "$CURRENT_DIR/timer_clear_page_cache.bash"
sudo -E bash "$CURRENT_DIR/run_etl.bash"
sudo -E bash "$CURRENT_DIR/install_nginx.bash"
sudo -E bash "$CURRENT_DIR/certbot_ca_certificate.bash"
sudo -E bash "$CURRENT_DIR/run_webapp.bash"
