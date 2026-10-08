docker compose down

# Reset the database.
rm -fr /tmp/paimon/*

# remove-orphans is for those cases where I modify the compose file.
docker compose up -d --no-recreate --remove-orphans

# router example: produce and store in data lake.
python ./producers/router.py > /dev/null

docker exec broker /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic sink_network_max_subflow
docker exec broker /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic sink_network_max_subflow_fine_grained

python ./src/init.py