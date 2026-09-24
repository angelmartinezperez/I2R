from pathlib import Path
import sys

from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.common.types import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.window import SlidingEventTimeWindows, Time

TOPIC = "wordcount_events_individual"
TOPIC_SINK = "sink_wordcount_max_k"
BOOTSTRAP_SERVER = "broker:9092"

word_count = Types.ROW_NAMED(
    ['id', 'timestamp', 'word', 'count'],
    [Types.INT(), Types.LONG(), Types.STRING(), Types.INT()]
)

json_deserializer = JsonRowDeserializationSchema.builder() \
    .type_info(word_count) \
    .build()

json_serializer = JsonRowSerializationSchema.builder() \
    .with_type_info(word_count) \
    .build()

def max_word_streaming():
    # I dont know which one of these settings makes it work. It is most probably a windows issue.
    config = Configuration()
    config.set_string("rest.address", "jobmanager")
    config.set_integer("rest.port", 8081)
    config.set_string("python.execution-mode", "process")

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_python_executable(sys.executable)
    env.set_parallelism(1)

    jar_uri = (Path(__file__).parent.parent / "dependencies" / "flink-sql-connector-kafka-5.0.0-2.2.jar").resolve().as_uri()
    env.add_jars(jar_uri)


    source = KafkaSource.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVER) \
        .set_topics(TOPIC) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(json_deserializer) \
        .build()

    # Kafka's timestamp is in event time.
    stream = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka topic", word_count)

    # The last window all + reduce could also be done with a max by if they were keyed by timestamp.
    # For that, a process window function would need to be used. (Similar to the training in the flink-training repo).
    max_word = stream \
        .key_by(lambda event: event.word, key_type=Types.STRING()) \
        .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(200))) \
        .reduce(lambda a, b: Row(id=0, timestamp=max(a.timestamp, b.timestamp), word=a.word, count=a.count + b.count)) \
        .window_all(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(200))) \
        .reduce(lambda a, b: a if a.count > b.count else b)

    serialization_schema = KafkaRecordSerializationSchema.builder() \
        .set_topic(TOPIC_SINK) \
        .set_value_serialization_schema(json_serializer) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVER) \
        .set_record_serializer(serialization_schema) \
        .build()

    max_word.sink_to(sink)

    env.execute()

if __name__ == '__main__':
    max_word_streaming()