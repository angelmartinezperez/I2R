from jobs.job import Job
from jobs.implementations.max_subflow import MaxSubflowJob
from jobs.constants.kafka import Topics

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.udf import ScalarFunction, udf
from pyflink.table.expressions import call, col


class MonitorOnSpike(ScalarFunction):
    """
    UDF for creating a new subflow measuring job when the value goes over a certain threshold.
    """
    def __init__(self, threshold, slide_ms):
        self.threshold = threshold
        self.slide_ms = slide_ms

    def eval(self, value): # type: ignore
        if (value > self.threshold):
            monitor_job = MaxSubflowJob(self.slide_ms, Topics.network_events, Topics.max_subflow_fine_grained)
            monitor_job.send()
        return value
            

class MonitorMaxJob(Job):
    """
    Monitors the max subflow and creates a new calculate max subflow job with finer granularity if it passes a certain threshold.
    """

    def __init__(self, threshold, slide_ms, source_topic = Topics.max_subflow):
        self.threshold = threshold
        self.slide_ms = slide_ms
        self.source_topic = source_topic

    def run(self):
        """
        Run the job.
        Intended to be called from the pyflink client (by the command flink run).
        This method should create and execute the environment.
        """
        env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        t_env = TableEnvironment.create(env_settings)

        source_ddl = self.source_topic.get_source_ddl()
        t_env.execute_sql(source_ddl)

        table_source = t_env.from_path(self.source_topic.name)

        monitor_on_spike = udf(MonitorOnSpike(self.threshold, self.slide_ms), result_type='INT')
        spikes = table_source \
            .where(col("count") > self.threshold) \
            .where(monitor_on_spike(col("count")) > 0) # Lazy way of triggering the method without selecting

        spikes.execute().print()
        


        
