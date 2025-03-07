docker stop attendance
docker rm attendance
docker rmi coramdeoyouth/attendance:prod
docker pull coramdeoyouth/attendance:prod
docker run -d -p 3000:3000 --name attendance -e NODE_ENV=production --restart unless-stopped coramdeoyouth/attendance:prod
