############### INSTRUCTIONS ###############

    # 1. Create the following tables on SQL

        # CREATE TABLE processed_events_aggregated_homework (
        # 	window_start TIMESTAMP(3),
        #   window_end TIMESTAMP(3),
        # 	host VARCHAR,
        # 	ip VARCHAR,
        # 	num_hits BIGINT
        # )

        # CREATE TABLE processed_events_aggregated_source_homework (
        # 	window_start TIMESTAMP(3),
        #   window_end TIMESTAMP(3),
        # 	host VARCHAR,
        # 	ip VARCHAR,
        # 	referrer VARCHAR,
        # 	num_hits BIGINT
        # )

        # By running an operation on window_end - window_start for the window delta, we can see that:
        # The minimum window time is 5 minutes, so a click followed by 5 minutes of inactivity is captured correctly
        # Whereas other windows with more clicks go over 5 minutes until 5 minutes of inactivity are reached.

    # 2. Update the Makefile with an entry:

        # homework_job:
        #       docker compose exec jobmanager ./bin/flink run -py /opt/src/job/homework_job.py --pyFiles /opt/src -d	
        # Run make homework_job to deploy the flink homework job

    # 3. Run the SQL queries in Postgres to answer

        # - What is the average number of web events of a session from a user on Tech Creator?
            # SELECT 
            # 	ROUND(AVG(web_events),2) AS web_events
            # FROM (
            # SELECT 
            # 	ip,
            # 	ROUND(AVG(num_hits),2) AS web_events
            # FROM processed_events_aggregated_homework 
            # WHERE host LIKE '%%techcreator%%'
            # GROUP BY ip
            # ) t

        # - Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)

            # SELECT 
            # 	ROUND(AVG(web_events),2) AS web_events,
            # 	host
            # FROM (
            # SELECT 
            # 	ip,
            # 	host,
            # 	ROUND(AVG(num_hits),2) AS web_events
            # FROM processed_events_aggregated_homework 
            # GROUP BY ip,host
            # ) t
            # GROUP BY host

############### Full flink job ###############

import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


def create_aggregated_events_sink_postgres(t_env):
    table_name = 'processed_events_aggregated_homework'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            host VARCHAR,
            ip VARCHAR,
            num_hits BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_aggregated_events_referrer_sink_postgres(t_env):
    table_name = 'processed_events_aggregated_source_homework'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            host VARCHAR,
            ip VARCHAR,
            referrer VARCHAR,
            num_hits BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_processed_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_processed_events_source_kafka(t_env)

        aggregated_table = create_aggregated_events_sink_postgres(t_env)
        aggregated_sink_table = create_aggregated_events_referrer_sink_postgres(t_env)
        t_env.from_path(source_table)\
            .window(
            Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w") # Create a Session window with 5min gap
        ).group_by( #group by window time, host, ip
            col("w"),
            col("host"),
            col("ip")
        ) \
            .select(
                    col("w").start.alias("window_start"), # Expand the window in start and end
                    col("w").end.alias("window_end"),
                    col("host"),
                    col("ip"),
                    col("host").count.alias("num_hits")
            ) \
            .execute_insert(aggregated_table)

        t_env.from_path(source_table).window(
            Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w") # Create a Session window with 5min gap
        ).group_by( #group by window, host, ip, referrer
            col("w"),
            col("host"),
            col("ip"),
            col("referrer")
        ) \
            .select(
                col("w").start.alias("window_start"), # Expand the window in start and end
                col("w").end.alias("window_end"),
                col("host"),
                col("ip"),
                col("referrer"),
                col("host").count.alias("num_hits")
        ) \
            .execute_insert(aggregated_sink_table) \
            .wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()
