events {

}

http {
  # HTTP: serve ACME challenges, redirect everything else to HTTPS
  server {
    listen 80;
    server_name ${NAMESPACE};

    location ~ /.well-known {
      root /etc/letsencrypt/live/${NAMESPACE}/;
    }
    # request_uri preserves query parameters such as communes or departements
    location / {
      return 301 https://${NAMESPACE}$request_uri;
    }
  }

  # HTTPS: terminate TLS, reverse proxy to the Flask app on 127.0.0.1:5000
  server {
    listen 443 ssl http2;
    server_name ${NAMESPACE};

    ssl_certificate     /etc/letsencrypt/live/${NAMESPACE}/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/${NAMESPACE}/privkey.pem;
    
    # proxy_set_header passes connection information to application on port 5000
    location / {
      proxy_pass http://127.0.0.1:5000;
      proxy_set_header Host $host;
      proxy_set_header X-Real-IP $remote_addr;
      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
      proxy_set_header X-Forwarded-Proto $scheme;
    }
  }
}

