events {

}

http {
  server {

    listen 80;
    server_name ${namespace};

    location ~ /.well-known {
      root /etc/letsencrypt/live/${namespace}/;
    }

    location / {
      return 301 https://${namespace};
    }
  }
}
