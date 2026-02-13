# 현재는 nginx 컨테이너 내부에서 관리 중
# 추후에는 머신에서 관리 + volume 마운트 방식으로
certbot certonly --nginx -d tychicus-dev.icoramdeo.com -d tychicus.icoramdeo.com -d tychicus-dashboard-dev.icoramdeo.com -d tychicus-dashboard.icoramdeo.com