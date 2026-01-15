#!/bin/bash
sudo snap install --classic certbot
sudo ln -s /snap/bin/certbot /usr/bin/certbot
sudo certbot --nginx --non-interactive --agree-tos -d dev.ousontlesabstentions.org
sudo cp /etc/letsencrypt/live/dev.ousontlesabstentions.org/privkey.pem /home/ubuntu/ou_sont_les_absentions/key.pem
sudo cp /etc/letsencrypt/live/dev.ousontlesabstentions.org/fullchain.pem /home/ubuntu/ou_sont_les_absentions/cert.pem
sudo rm /etc/nginx/sites-enabled/default
sudo fuser -k 80/tcp