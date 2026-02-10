#!/usr/bin/env python

from argparse import ArgumentParser
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from kafka_utils import wait_for_kafka_topics

from pyspark.sql.functions import (
    col,
    from_json, 
    when,
    lower,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


JDBC_URL = "jdbc:postgresql://postgres:5432/db"
JDBC_PROPERTIES = {
    "user": "user",
    "password": "user",
    "driver": "org.postgresql.Driver"
}


def main():

    # Parse command line options (call program with --help to see supported options)
    parser = ArgumentParser(description="Spark processor processing Open Library recent changes")
    parser.add_argument("--bootstrap-servers", default="broker:9092", help="Kafka bootstrap servers", type=str)
    parser.add_argument("--topic-input", default="ol.recentchanges", type=str)
    parser.add_argument("--dry-run", help="print results to stdout instead of writing them back to Kafka", action="store_true")
    args = parser.parse_args()

    # Wait for input Kafka topic to be created externally. We may proceed also if they are absent, but in
    # that case they will be created with default settings (e.g., no cleanup of data older than 5 minutes)
    wait_for_kafka_topics(args.bootstrap_servers, args.topic_input)

    # Obtain the SparkSession, tuning shuffle partitions and adding the Kafka connector
    spark: SparkSession = (
        SparkSession.builder
        .appName("openlibrary-processor")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
        # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )

    # Set log4j log level of Spark engine to WARN (useful when submitted to a cluster, to make logs less verbose)
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("id", StringType()),
        StructField("kind", StringType()),
        StructField("timestamp", StringType()),
        StructField("comment", StringType()),
        StructField(
            "author",
            StructType([
                StructField("key", StringType())
            ])
        ),
        StructField("ip", StringType()),
        StructField("data", StructType([]))
    ])

    # Read the 'page_create_event' and 'page_delete_event' dataframes from Kafka corresponding topics
    df_event = read_dataframe(spark, args, args.topic_input, schema)

    # Register dataframes as streaming tables into Spark catalog
    df_event.createOrReplaceTempView("events")

    # Compute sliding window (length 1', slide 5") count of pages created total / by bots, per wiki
    df_per_minute = spark.sql(
        """
        SELECT  WINDOW(event_time, '1 minute').start    AS window_start,
                WINDOW(event_time, '1 minute').end      AS window_end,
                COUNT(*)                                AS count
        FROM    events
        GROUP BY WINDOW(event_time, '1 minute')
        """
    )

    # Compute sliding window (length 1', slide 5") count of pages created - pages deleted
    df_by_kind = spark.sql(
        """
        SELECT  kind,
                COUNT(*) AS count
        FROM    events
        GROUP BY kind
        """
    )

    # Compute sliding window (length 1', slide 5") count of pages created - pages deleted
    df_by_actor_type = spark.sql(
        """
        SELECT  actor_type,
                COUNT(*) AS count
        FROM    events
        GROUP BY actor_type
        """
    )


    # Start queries, writing results to either stdout or kafka
    # (note: would have been more efficient to do a unionAll and write the result with mode=update,
    # but unionAll after aggregations is not supported in Spark Structured Streaming)
    queries: List[Optional[StreamingQuery]] = []
    queries.append(
        write_dataframe(
            args,
            df_per_minute,
            "changes_per_minute",
            "append",
            "append"
        )
    )
    queries.append(
        write_dataframe(
            args,
            df_by_kind,
            "changes_by_kind",
            "complete",
            "overwrite"
        )
    )
    queries.append(
        write_dataframe(
            args,
            df_by_actor_type,
            "changes_by_actor_type",
            "complete",
            "overwrite"
        )
    )

    # Wait for queries termination
    for query in queries:
        if query:
            query.awaitTermination()


def read_dataframe(spark: SparkSession, args, topic: str, schema: StructType):

    # Read page create/delete events from specified Kafka 'topic', according to Wikipedia JSON structure, see
    # - https://stream.wikimedia.org/?doc#/streams/get_v2_stream_page_create for create events,
    # - https://stream.wikimedia.org/?doc#/streams/get_v2_stream_page_delete for delete events
    #
    # Notes:
    # - we limit reading JSON fields/sub-fields of interest that are common to the 2 types of event
    #   (beware of wrong datatypes -> will get a whole 'null' struct)
    # - we map the ISO timestamp string in 'meta.dt' to a TIMESTAMP column, as this is required for watermark,
    #   join and window operations
    # - regarding watermarks, we assume data may arrive 1 minute late at most (assumption that seems to work)

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", False)
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("e"))
        .select(
            col("e.kind"),
            when(
                col("e.author.key").isNotNull() &
                lower(col("e.author.key")).contains("bot"),
                "bot"
            )
            .when(col("e.ip").isNotNull(), "anonymous")
            .otherwise("user")
            .alias("actor_type"),
            col("e.timestamp").cast("timestamp").alias("event_time")
        )
        .withWatermark("event_time", "2 minutes")
    )

        # .selectExpr("CAST(value AS STRING)")
        # .selectExpr("""from_json(value, '
        #     meta STRUCT<uri: STRING, dt: STRING>,
        #     database STRING,
        #     page_id INTEGER,
        #     page_title STRING,
        #     page_is_redirect BOOLEAN,
        #     rev_id INTEGER,
        #     performer STRUCT<user_text: STRING, user_is_bot: BOOLEAN>
        # ') AS value""")
        # .selectExpr("value.*")  # flattening
        # .filter("regexp(meta.uri, '.*\.wikipedia\.org/wiki/.*')")
        # .selectExpr(
        #     "CAST(to_timestamp(meta.dt, 'yyyy-MM-dd\\'T\\'HH:mm:ssX') AS TIMESTAMP) AS ts",
        #     "database AS wiki",
        #     "meta.uri AS uri",
        #     "page_id",
        #     "page_title",
        #     "page_is_redirect AS page_is_redirect",
        #     "rev_id",
        #     "performer.user_text AS user",
        #     "performer.user_is_bot AS user_is_bot"
        # )
        # .withWatermark("ts", "1 MINUTE") )

    print(f"'{topic}' dataframe schema:")
    df.printSchema()

    return df


def write_dataframe(args, df: Optional[DataFrame], table_name: str, mode="complete", foreach_mode="overwrite", trigger="5 seconds") -> StreamingQuery:

    if not df:
        # If the query has not been implemented yet, do nothing
        return None

    elif args.dry_run:
        # If not and if dry-run, start the query and print results on stdout
        return (
            df.writeStream
            .outputMode(mode)
            .format("console")
            .option("truncate", False)
            .trigger(processingTime=trigger)
            .start()
        )
    else:
        return (
            df.writeStream
            .outputMode(mode)
            .foreachBatch(
                lambda df, _: df.write
                    .mode(foreach_mode)
                    .jdbc(
                        url=JDBC_URL,
                        table=table_name,
                        properties=JDBC_PROPERTIES
                    )
            )
            .start()
        )


if __name__ == "__main__":
    main()
