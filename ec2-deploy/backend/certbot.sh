sudo certbot certonly --standalone \
  --preferred-challenges http \
  --http-01-address 0.0.0.0 \
  -d attendance-dev.icoramdeo.com -d attendance.icoramdeo.com -d ym-back.icoramdeo.com \
  --renew-with-new-domains \
  --webroot-path /var/www/certbot