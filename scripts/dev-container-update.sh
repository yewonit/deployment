docker stop attendance-dev
docker rm attendance-dev
docker rmi coramdeoyouth/attendance:dev
docker pull coramdeoyouth/attendance:dev
docker run -d -p 3001:3000 --name attendance-dev -e NODE_ENV=development --restart unless-stopped coramdeoyouth/attendance:dev
