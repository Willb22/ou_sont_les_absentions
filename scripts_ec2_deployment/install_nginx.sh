#!/bin/bash
sudo apt update
sudo apt install nginx gettext-base -y # gettext-base provides envsubst
#sudo ufw app list
sudo ufw allow 'Nginx HTTP'
sudo ufw allow OpenSSH # Do not block ssh into ec2 for now
#sudo ufw status
#sudo ufw --force enable # Uncomment to enable Firewall
sudo ufw reload
#systemctl status nginx
sudo bash -c 'envsubst < /home/ubuntu/ou_sont_les_absentions/web_app/nginx_conf/nginx.conf.tpl > /etc/nginx/nginx.conf'