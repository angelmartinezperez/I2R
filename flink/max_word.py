from pathlib import Path
import sys

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.types import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.window import SlidingEventTimeWindows, Time


word_count = Types.ROW_NAMED(
    ['id', 'timestamp', 'word', 'count'],
    [Types.INT(), Types.LONG(), Types.STRING(), Types.INT()]
)

json_deserializer = JsonRowDeserializationSchema.builder() \
    .type_info(word_count) \
    .build()

def max_word_streaming():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_python_executable(sys.executable)

    jar_uri = Path("./dependencies/flink-sql-connector-kafka-5.0.0-2.2.jar").resolve().as_uri()
    env.add_jars(jar_uri)

    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("wordcount_events_individual") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(json_deserializer) \
        .build()

    # Kafka's timestamp is in event time.
    stream = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka topic", word_count)

    max_word = stream \
        .key_by(lambda event: event.word, key_type=Types.STRING()) \
        .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(200))) \
        .reduce(lambda a, b: Row(id=a.id, word=a.word, count=a.count + b.count)) \
        .print()

    env.execute()

if __name__ == '__main__':
    max_word_streaming()