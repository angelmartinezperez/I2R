from pathlib import Path

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

TOPIC = "wordcount_events_individual"
BOOTSTRAP_SERVERS = "broker:9092"

def read_kafka_streaming():
    # Create a streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    jar_uri = Path("./dependencies/flink-sql-connector-kafka-5.0.0-2.2.jar").resolve().as_uri()
    env.add_jars(jar_uri)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
        .set_topics(TOPIC) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    data_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    data_stream.print()

    env.execute("Read Kafka")

if __name__ == '__main__':
    read_kafka_streaming()