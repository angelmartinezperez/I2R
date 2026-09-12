# I2R

## Python env
PyFlink needs 3.9, 3.10, 3.11 or 3.12.

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

