import logging
import os
import sys
from datetime import datetime, timedelta

from env_vars import data_root_path
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)
from pyspark.sql.window import Window

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
        .config("spark.cores.max", "2")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
# CONFIG SPARK #############################################


########################################################################
# READ DATA ############################################################
schema_events = StructType([
    StructField("customer_id", LongType(), True),
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("event_type", StringType(), True),
])

df_events = (
    spark.readStream
        .format("parquet")
        .schema(schema_events)
        .option("startingOffsets", "earliest")
        .load(f"{data_root_path}/raw_data/events")
)


###########################################################################
# PROCESS DATA ############################################################

df_cleaned_events = (
    df_events
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["customer_id", "event_timestamp"])
)


# #######################################################################
# WRITE DATA ############################################################
streaming_valid_events = (
    df_cleaned_events.writeStream
        .format("parquet")
        .option("path", f"{data_root_path}/output/fact_events_streaming")
        .option("checkpointLocation", f"{data_root_path}/checkpoint_data/clean_fact_events_streaming")
        .partitionBy("processing_date")
        .outputMode("append")
        .trigger(processingTime='3 seconds')
        .start()
)

streaming_valid_events.awaitTermination()
