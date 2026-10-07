import sys

from jobs.job import Job
from jobs.constants.kafka import Topics

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.types import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.window import SlidingEventTimeWindows, Time


class MaxSubflowJob(Job):
    """Calcualte the maximum subflow with a sliding window."""

    def __init__(self, slide_ms, source_topic=Topics.network_events, sink_topic=Topics.max_subflow):
        self.slide_ms = slide_ms
        self.source_topic = source_topic
        self.sink_topic = sink_topic

    def run(self):
        """
        Run the job.
        Intended to be called from the pyflink client (by the command flink run).
        This method should create and execute the environment.
        """
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_python_executable(sys.executable)
        env.set_parallelism(1)

        # Kafka's timestamp is in event time.
        source = self.source_topic.get_source()
        stream = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka topic", self.source_topic.type_datastream())

        max_subflow = stream \
            .key_by(lambda event: f"{event.ip_src}|{event.ip_dst}", key_type=Types.STRING()) \
            .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(self.slide_ms))) \
            .reduce(lambda a, b: Row(
                id=0,
                timestamp=max(a.timestamp, b.timestamp),
                ip_src=a.ip_src,
                ip_dst=a.ip_dst,
                count=a.count + b.count,
            )) \
            .window_all(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(self.slide_ms))) \
            .reduce(lambda a, b: a if a.count > b.count else b)

        sink = self.sink_topic.get_sink()
        max_subflow.sink_to(sink)

        env.execute("Max Subflow")

        