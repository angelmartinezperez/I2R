# I2R

## Set up
The devcontainer and containers need to share a docker network for communication.
The easiest way to do this is to run 'docker compose up -d' first and then the devcontainer.
Running the devcontainer will fail if the network does not exist yet.

## Kafka docker
Start with vs code menu.
kafka is in /opt/kafka.
Bootstrap server: localhost:9092.
Example commands:
- Go to the kafka binaries: 'cd /opt/kafka/bin'
- List topics: './kafka-topics.sh --list --bootstrap-server localhost:9092'

## Example execution
Following https://kafka.apache.org/quickstart/

1. Start kafka container
2. Go to the kafka binaries: 'cd /opt/kafka/bin'
3. Create topic if it does not exist (optional, auto create is true by default): './kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092'
4. Run example.
5. Read the events: './kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092'

## CMS Router
See topic: ./kafka-console-consumer.sh --topic wordcount_events --from-beginning --bootstrap-server localhost:9092

## To do
Clean constants and DTOs (put them in a single file).
Programs have to be executed in their folder because of the paths. Fix.

## Questions
Is it good practice to include the key and timestamp inside the event payload?
    It duplicates data but it is gonna have to be included anyway by flink to process it.