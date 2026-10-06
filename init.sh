docker compose down

# Reset the database.
rm -fr /tmp/paimon/*

# remove-orphans is for those cases where I modify the compose file.
docker compose up -d --no-recreate --remove-orphans

# wordcount example.
python ./producers/CMS/cms_router_individual.py > /dev/null
docker exec broker /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic sink_wordcount_events_individual
docker exec broker /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic sink_wordcount_max_k

# router example: produce and store in data lake.
python ./producers/router.py > /dev/null
python ./src/init.py