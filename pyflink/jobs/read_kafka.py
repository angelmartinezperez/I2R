from pathlib import Path

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema

TOPIC = "wordcount_events_individual"
TOPIC_SINK = "sink_wordcount_events_individual"
BOOTSTRAP_SERVER = "broker:9092"

def read_kafka_streaming():
    env = StreamExecutionEnvironment.get_execution_environment()

    source = KafkaSource.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVER) \
        .set_topics(TOPIC) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    serialization_schema = KafkaRecordSerializationSchema.builder() \
        .set_topic(TOPIC_SINK) \
        .set_value_serialization_schema(SimpleStringSchema()) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVER) \
        .set_record_serializer(serialization_schema) \
        .build()

    data_stream = env \
        .from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source") \
        .sink_to(sink)

    env.execute("Read Kafka")

if __name__ == '__main__':
    read_kafka_streaming()