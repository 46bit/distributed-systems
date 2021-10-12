docker rm -f redis-1
docker rm -f redis-2
docker rm -f redis-3

docker run --name redis-1 -p 127.0.0.1:6379:6379 --ulimit nofile=5000:5000 -d redis:alpine
docker run --name redis-2 -p 127.0.0.1:6380:6379 --ulimit nofile=5000:5000 -d redis:alpine
docker run --name redis-3 -p 127.0.0.1:6381:6379 --ulimit nofile=5000:5000 -d redis:alpine

sleep 1

echo "8081
8082
8083" | parallel -j 20 --lb -X ./scripts/load-once.sh
