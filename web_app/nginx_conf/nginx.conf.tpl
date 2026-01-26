events {

}

http {
  server {

    listen 80;
    server_name ${NAMESPACE};

    location ~ /.well-known {
      root /etc/letsencrypt/live/${NAMESPACE}/;
    }

    location / {
      return 301 https://${NAMESPACE};
    }
  }
}
