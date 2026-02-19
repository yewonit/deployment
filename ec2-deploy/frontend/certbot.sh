sudo certbot certonly --standalone \
  --preferred-challenges http \
  --http-01-address 0.0.0.0 \
  -d tychicus-dev.icoramdeo.com -d tychicus.icoramdeo.com -d tychicus-dashboard-dev.icoramdeo.com -d tychicus-dashboard.icoramdeo.com \
  --renew-with-new-domains \
  --webroot-path /var/www/certbot
