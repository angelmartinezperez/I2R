import sys

from jobs.job import Job
from jobs.constants.kafka import KafkaConstants

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.types import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.window import SlidingEventTimeWindows, Time


class MaxSubflowJob(Job):
    """Calcualte the maximum subflow with a sliding window."""

    def run(self):
        """
        Run the job.
        Intended to be called from the pyflink client (by the command flink run).
        This method should create and execute the environment.
        """
        subflow_count = Types.ROW_NAMED(
            ['id', 'timestamp', 'ip_src', 'ip_dst', 'count'],
            [Types.INT(), Types.LONG(), Types.STRING(), Types.STRING(), Types.INT()]
        )

        json_deserializer = JsonRowDeserializationSchema.builder().type_info(subflow_count).build()
        json_serializer = JsonRowSerializationSchema.builder().with_type_info(subflow_count).build()

        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_python_executable(sys.executable)
        env.set_parallelism(1)

        source = KafkaSource.builder() \
            .set_bootstrap_servers(KafkaConstants.bootstrap_server) \
            .set_topics(KafkaConstants.network_events_topic) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(json_deserializer) \
            .build()

        # Kafka's timestamp is in event time.
        stream = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka topic", subflow_count)

        max_subflow = stream \
            .key_by(lambda event: f"{event.ip_src}|{event.ip_dst}", key_type=Types.STRING()) \
            .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(200))) \
            .reduce(lambda a, b: Row(
                id=0,
                timestamp=max(a.timestamp, b.timestamp),
                ip_src=a.ip_src,
                ip_dst=a.ip_dst,
                count=a.count + b.count,
            )) \
            .window_all(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(200))) \
            .reduce(lambda a, b: a if a.count > b.count else b)

        serialization_schema = KafkaRecordSerializationSchema.builder() \
            .set_topic(KafkaConstants.max_subflow_topic) \
            .set_value_serialization_schema(json_serializer) \
            .build()

        sink = KafkaSink.builder() \
            .set_bootstrap_servers(KafkaConstants.bootstrap_server) \
            .set_record_serializer(serialization_schema) \
            .build()

        max_subflow.sink_to(sink)

        env.execute("Max Subflow")

        