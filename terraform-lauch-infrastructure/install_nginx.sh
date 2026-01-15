#!/bin/bash
sudo apt update
sudo apt install nginx -y
#sudo ufw app list
sudo ufw allow 'Nginx HTTP'
sudo ufw allow OpenSSH # Do not block ssh into ec2 for now
#sudo ufw status
#sudo ufw --force enable # Uncomment to enable Firewall
sudo ufw reload
#systemctl status nginx
