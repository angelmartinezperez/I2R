import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def word_count_streaming():
    # Create a streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Create a table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, settings)

    test_file = os.path.abspath("test.txt")
    file_uri = "file:///" + test_file.replace("\\", "/")
    print(f"Reading from: {file_uri}")

    source_ddl = f"""
        CREATE TABLE source (
            word STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{file_uri}',
            'format' = 'csv',
            'source.monitor-interval' = '1s'
        )
    """

    # Create a sink table to print results
    sink_ddl = """
        CREATE TABLE sink (
            word STRING,
            word_count BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """

    # Execute DDL statements
    table_env.execute_sql(source_ddl)
    table_env.execute_sql(sink_ddl)

    # Create a query and **wait** for the job to run
    table_result = table_env.sql_query("""
        SELECT word, COUNT(*) as word_count
        FROM source
        GROUP BY word
    """).execute_insert('sink')

    # Block until the job finishes (or, for a streaming job, keep the script alive)
    table_result.wait()

if __name__ == '__main__':
    word_count_streaming()