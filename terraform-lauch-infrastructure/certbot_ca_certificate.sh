#!/bin/bash
LE_LIVE_DIR="/etc/letsencrypt/live/$NAMESPACE"

echo "Checking for existing Let's Encrypt certificate..."

if [ -f "$LE_LIVE_DIR/privkey.pem" ] && [ -f "$LE_LIVE_DIR/fullchain.pem" ]; then
  echo "Certificate already exists — skipping certbot"
else
  echo "Requesting new certificate..."

  nginx -t
  systemctl restart nginx

  snap install --classic certbot
  ln -sf /snap/bin/certbot /usr/bin/certbot

  certbot --nginx \
    --non-interactive \
    --agree-tos \
    -d "$NAMESPACE"
fi
nginx -t && systemctl restart nginx

cp "$LE_LIVE_DIR/privkey.pem" "/home/ubuntu/ou_sont_les_absentions/key.pem"
cp "$LE_LIVE_DIR/fullchain.pem" "/home/ubuntu/ou_sont_les_absentions/cert.pem"
sudo chown ubuntu:ubuntu /home/ubuntu/ou_sont_les_absentions/key.pem
sudo chown ubuntu:ubuntu /home/ubuntu/ou_sont_les_absentions/cert.pem

rm -f /etc/nginx/sites-enabled/default
sudo fuser -k 80/tcp
