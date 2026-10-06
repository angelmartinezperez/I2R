from jobs.topic_to_lake import TopicToLakeJob
from pyflink.datastream.functions import MapFunction

class CreateSubjobFunction(MapFunction):
    # For testing purposes, send a job when the function is created.
    def open(self, runtime_context):
        subjob = TopicToLakeJob()
        subjob.send()

    def map(self, value):
        return value