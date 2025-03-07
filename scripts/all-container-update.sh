docker stop attendance-dev
docker rm attendance-dev
docker stop attendance
docker rm attendance
docker rmi $(docker images -q)
docker pull coramdeoyouth/attendance:dev
docker pull coramdeoyouth/attendance:prod
docker run -d -p 3001:3000 --name attendance-dev -e NODE_ENV=development --restart unless-stopped coramdeoyouth/attendance:dev
docker run -d -p 3000:3000 --name attendance -e NODE_ENV=production --restart unless-stopped coramdeoyouth/attendance:prod
