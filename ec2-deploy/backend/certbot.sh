# 현재는 nginx 컨테이너 내부에서 관리 중
# 추후에는 머신에서 관리 + volume 마운트 방식으로
certbot certonly --nginx -d attendance-dev.icoramdeo.com -d attendance.icoramdeo.com -d ym-back.icoramdeo.com