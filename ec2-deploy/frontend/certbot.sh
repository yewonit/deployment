# 플러그인 설치 (Ubuntu 기준)
sudo apt update
sudo apt install python3-certbot-dns-route53 -y

# 인증서 발급 명령어
sudo certbot certonly --dns-route53 \
  -d tychicus-dev.icoramdeo.com \
  -d tychicus.icoramdeo.com \
  -d tychicus-dashboard-dev.icoramdeo.com \
  -d tychicus-dashboard.icoramdeo.com