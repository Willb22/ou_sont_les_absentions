#!/bin/bash
#set -e #Comment here to let script pursue after a failed line

# Ensure env is loaded
export $(grep -v '^#' /home/ubuntu/ou_sont_les_absentions/.env | xargs)

./scripts/install_docker_docker_compose.sh
./scripts/restore_backup_https_cert.sh
./scripts/timer_clear_page_cache.sh
./scripts/run_etl.sh
./scripts/install_nginx.sh
./scripts/certbot_ca_certificate.sh
./scripts/run_webapp.sh
