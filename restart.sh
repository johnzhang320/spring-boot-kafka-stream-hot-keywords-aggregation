docker compose down --remove-orphans
docker container rm -f zookeeper
docker container rm -f kafka
docker compose up --remove-orphans
