import logging
import os
import sys
from datetime import datetime, timedelta

from env_vars import data_root_path
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

# LOG ######################################################
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

logger = logging.getLogger(__name__)
# LOG ######################################################

# CONFIG SPARK #############################################
spark = (
    SparkSession.builder
        .appName("events_job")
        .master("spark://spark-master:7077")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
# CONFIG SPARK #############################################


# PROCESS DATA

schema_events = StructType([
    StructField("customer_id", LongType(), True),
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
])

df_events = (
    spark.readStream
        .schema(schema_events)
        .format("json")
        .load(f"{data_root_path}/input_streaming/events")
)

# when the event was processed
df_events_transform = (
    df_events
        .withColumn("processing_date", f.current_date())
        .withColumn("processing_timestamp", f.current_timestamp())
)


# process and validating data
_timestamp_format = "yyyy-MM-dd'T'HH:mm:ssX"

df_validate_regs = df_events_transform.select(
        f.col("event_id"),
        f.col("customer_id"),
        f.upper(f.regexp_replace(f.col("event_type"), r"[^a-zA-Z]", "")).alias("event_type"), # transform and clean
        f.try_to_timestamp(f.col("event_timestamp"), f.lit(_timestamp_format)).alias("event_timestamp"), # enforce timestap format
        f.col("processing_date"),
        f.col("processing_timestamp"),
)


# get valid events
df_valid_events = df_validate_regs.filter(f.col("event_timestamp").isNotNull())

# get invalid events
df_invalid_events = df_validate_regs.filter(f.col("event_timestamp").isNull())


streaming_valid_events = (
    df_valid_events.writeStream
        .format("parquet")
        # .option("header", "true")
        .option("path", f"{data_root_path}/raw_data/events")
        .option("checkpointLocation", f"{data_root_path}/raw_data/events/checkpoints/clean_events")
        .partitionBy("processing_date")
        .outputMode("append")
        .trigger(processingTime='1 seconds')
        .start()
)

streaming_error_events = (
    df_invalid_events.writeStream
        .format("parquet")
        # .option("header", "true")
        .option("path", f"{data_root_path}/rejected_data/events")
        .option("checkpointLocation", f"{data_root_path}/rejected_data/events/checkpoints/error_events")
        .partitionBy("processing_date")
        .outputMode("append")
        .trigger(processingTime='1 seconds')
        .start()
)

spark.streams.awaitAnyTermination()
