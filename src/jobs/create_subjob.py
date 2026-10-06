import sys

from pyflink.common import Types
from pyflink.datastream import MapFunction, StreamExecutionEnvironment

from jobs.job import Job
from jobs.topic_to_lake import TopicToLakeJob

class CreateSubjobFunction(MapFunction):
    # For testing purposes, send a job when the function is created.
    def open(self, runtime_context):
        subjob = TopicToLakeJob()
        subjob.send()

    def map(self, value):
        return value

class CreateSubjobJob(Job):
    """Create a subjob."""

    def run(self):
        """
        Run the job.
        Intended to be called from the pyflink client (by the command flink run).
        This method should create and execute the environment.
        """
        # Very simple test job that creates a subjob when it is run.
        # The job is intended to be short lived and send the job on the operator open.
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_python_executable(sys.executable)
        env.set_parallelism(1)

        data_stream = env.from_collection([1, 2, 3, 4, 5], type_info=Types.INT())
        mapped_stream = data_stream.map(CreateSubjobFunction(), output_type=Types.INT())
        mapped_stream.print()

        env.execute("Create Subjob Job")