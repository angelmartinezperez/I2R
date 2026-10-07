from abc import ABC, abstractmethod

from pyflink.common import TypeInformation, Types
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema

class KafkaTopic():
    """Base topic class"""
    bootstrap_server = "broker:9092"

    def __init__(self, name):
        self.name = name


class TypedTopic(KafkaTopic):
    """Abstract base class for topics with typed messages."""

    @abstractmethod
    def type_datastream(self) -> TypeInformation:
        pass

    @abstractmethod
    def type_ddl(self) -> str:
        pass

    def get_json_serializer(self):
        return JsonRowSerializationSchema.builder().with_type_info(self.type_datastream()).build()
    
    def get_json_deserializer(self):
        return JsonRowDeserializationSchema.builder().type_info(self.type_datastream()).build()
    
    def get_source(self):
        source = KafkaSource.builder() \
            .set_bootstrap_servers(self.bootstrap_server) \
            .set_topics(self.name) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(self.get_json_deserializer()) \
            .build()

        return source
    
    def get_sink(self):
        serialization_schema = KafkaRecordSerializationSchema.builder() \
            .set_topic(self.name) \
            .set_value_serialization_schema(self.get_json_serializer()) \
            .build()

        sink = KafkaSink.builder() \
            .set_bootstrap_servers(self.bootstrap_server) \
            .set_record_serializer(serialization_schema) \
            .build()

        return sink

class FlowTopic(TypedTopic):
    """Topic with flow typed messages."""

    def __init__(self, name):
        super(FlowTopic, self).__init__(name)

    def type_datastream(self) -> TypeInformation:
        return Types.ROW_NAMED(
            ['id', 'timestamp', 'ip_src', 'ip_dst', 'count'],
            [Types.INT(), Types.LONG(), Types.STRING(), Types.STRING(), Types.INT()]
        )

    def type_ddl(self) -> str:
        return "id INT, timestamp LONG, ip_src STRING, ip_dst STRING, count INT"


class Topics():
    """Class containing all topics"""
    network_events = FlowTopic("network_events_individual")
    max_subflow = FlowTopic("sink_network_max_subflow")
    max_subflow_fine_grained = FlowTopic("sink_network_max_subflow_fine_grained")