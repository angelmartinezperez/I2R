import sys
import os.path
import time

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.number_seq import NumberSequenceSource

import jobs
from jobs.job import Job

from pyflink.datastream.functions import MapFunction

class PrintConsole(MapFunction):
    # For testing purposes, send a job when the function is created.
    def open(self, runtime_context):
        # Print pythonpath
        print("AAAA Python path:", sys.path)
        print("AAAA File:", __file__)
        print("AAAA Jobs module path:", jobs.__file__)
        print("AAAA Jobs module abs path:", os.path.abspath(jobs.__file__))

    def map(self, value):
        time.sleep(1)
        return value
    
class PrintConsoleJob(Job):
    """Create a subjob."""

    def run(self):
        """
        Run the job.
        Intended to be called from the pyflink client (by the command flink run).
        This method should create and execute the environment.
        """
        # Very simple test job that creates a subjob when it is run.
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_python_executable(sys.executable)
        env.set_parallelism(1)

        source = NumberSequenceSource(1, 2**63 - 1)
        data_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "numbers", type_info=Types.LONG())
        mapped_stream = data_stream.map(PrintConsole(), output_type=Types.INT())
        mapped_stream.print()

        env.execute("Create Subjob Job")