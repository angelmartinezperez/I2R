docker compose up -d --no-recreate
python ./producers/CMS/cms_router_individual.py > /dev/null
docker exec broker /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic sink_wordcount_events_individual
docker exec broker /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic sink_wordcount_max_k