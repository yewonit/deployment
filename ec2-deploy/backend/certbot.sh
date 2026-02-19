# 플러그인 설치 (Ubuntu 기준)
sudo yum update
sudo yum install python3-certbot-dns-route53 -y

sudo curl -s https://raw.githubusercontent.com/certbot/certbot/master/certbot-nginx/certbot_nginx/_internal/tls_configs/options-ssl-nginx.conf -o /etc/letsencrypt/options-ssl-nginx.conf
sudo openssl dhparam -out /etc/letsencrypt/ssl-dhparams.pem 2048

# 인증서 발급 명령어
sudo certbot certonly --dns-route53 \
  -d attendance-dev.icoramdeo.com \
  -d attendance.icoramdeo.com \
  -d ym-back.icoramdeo.com