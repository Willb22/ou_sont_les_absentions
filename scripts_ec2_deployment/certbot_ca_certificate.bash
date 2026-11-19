#!/bin/bash

LE_LIVE_DIR="/etc/letsencrypt/live/$NAMESPACE"

echo "Checking for existing Let's Encrypt certificate..."

if [ -f "$LE_LIVE_DIR/privkey.pem" ] && [ -f "$LE_LIVE_DIR/fullchain.pem" ]; then
  echo "Certificate already exists — skipping certbot"
else
  echo "Requesting new certificate..."

  nginx -t || exit 1
  systemctl restart nginx || exit 1

  max_attempts=4
  attempt=1
  until snap install --classic certbot; do
    if [ "$attempt" -ge "$max_attempts" ]; then
      echo "ERROR: Failed to install Certbot with Snap after $max_attempts attempts." >&2
      exit 1
    fi

    delay=$((attempt * 10))
    echo "WARNING: Certbot Snap installation failed (attempt $attempt/$max_attempts); retrying in ${delay}s." >&2
    sleep "$delay"
    attempt=$((attempt + 1))
  done

  ln -sf /snap/bin/certbot /usr/bin/certbot

  certbot --nginx \
    --non-interactive \
    --agree-tos \
    -d "$NAMESPACE" || exit 1
fi

envsubst '$NAMESPACE' \
  < /home/ubuntu/ou_sont_les_absentions/web_app/nginx_conf/nginx.conf.tpl \
  > /etc/nginx/nginx.conf || exit 1

nginx -t || exit 1
systemctl restart nginx || exit 1

rm -f /etc/nginx/sites-enabled/default
