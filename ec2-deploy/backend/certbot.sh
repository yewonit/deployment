# 플러그인 설치 (Ubuntu 기준)
sudo yum update
sudo yum install python3-certbot-dns-route53 -y

# 인증서 발급 명령어
sudo certbot certonly --dns-route53 \
  -d attendance-dev.icoramdeo.com \
  -d attendance.icoramdeo.com \
  -d ym-back.icoramdeo.com