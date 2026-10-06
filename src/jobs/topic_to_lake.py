from jobs.job import Job

from pyflink.table import TableEnvironment, EnvironmentSettings

class TopicToLakeJob(Job):
    """Send topic data to the lake."""

    def run(self):
        """
        Run the job.
        Intended to be called from the pyflink client (by the command flink run).
        This method should create and execute the environment.
        """
        # Create the table environment.
        env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        t_env = TableEnvironment.create(env_settings)
        # Checkpointing is necessary for paimon to commit the data.
        t_env.get_config().set('execution.checkpointing.interval', '1 s')

        # The pyflink API allows to do this in a probably cleaner way.
        t_env.execute_sql("CREATE CATALOG lake_catalog WITH ('type' = 'paimon', 'warehouse' = 'file:/tmp/paimon');")
        t_env.execute_sql("USE CATALOG lake_catalog;")
        t_env.execute_sql("""
            CREATE TEMPORARY TABLE events_source (
                `id` INT, 
                `timestamp` BIGINT, 
                `ip_src` STRING,
                `ip_dst` STRING,
                `count` INT
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'network_events_individual',
                'properties.bootstrap.servers' = 'broker:9092',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            );""")
        t_env.execute_sql("""
            CREATE TABLE IF NOT EXISTS events (
                `id` INT, 
                `timestamp` BIGINT, 
                `ip_src` STRING,
                `ip_dst` STRING,
                `count` INT
            );""")
        t_env.execute_sql("INSERT INTO events SELECT * FROM events_source;")

if __name__ == "__main__":
    job = TopicToLakeJob()
    job.run()