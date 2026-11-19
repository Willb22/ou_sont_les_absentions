events {
}

http {
  server {
    listen 80;
    server_name ${NAMESPACE};

    location / {
      return 404;
    }
  }
}
