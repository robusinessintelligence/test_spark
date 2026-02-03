import json
import logging
import os
import sys
from datetime import datetime, timedelta

from dateutil.rrule import MONTHLY, rrule
from env_vars import data_root_path
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
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
        .appName("customers_job")
        .master("spark://spark-master:7077")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# SPARK CONFIGS
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# CONFIG SPARK #############################################

start_time = datetime.now()


########################################################################
# READ DATA ############################################################

topic = "yellow_taxi_trip"
bronze_path = f"{data_root_path}/bronze/{topic}"

try:

    df_source = (
        spark.read
            .format("parquet")
            .load(bronze_path)
    )

except Exception as err:
    logger.info(f"error on reading file")
    raise err


###########################################################################
# PROCESS DATA ############################################################

df_source = df_source.withColumn("processing_date", f.current_date())

total_secs = (
    f.col("tpep_dropoff_datetime").cast("timestamp").cast("long") 
    - f.col("tpep_pickup_datetime").cast("timestamp").cast("long")
)

df_transform = df_source.withColumn(
    "tpep_total_trip_time_str",
    f.format_string(
        "%02d:%02d:%02d",
        (total_secs / 3600).cast("int"),
        ((total_secs % 3600) / 60).cast("int"),
        (total_secs % 60).cast("int"),
    )
)


df_transform_2 = df_transform.withColumn(
    "tpep_total_trip_time_secs",
    total_secs
)


df_transform_3 = df_transform_2.withColumn(
    "trip_id",
    f.md5(f.concat_ws("|", "VendorID", "tpep_pickup_datetime", "PULocationID"))
)

df_transform_3.printSchema()

df_filter = df_transform_3.filter((f.col("total_amount") > 0) & (f.col("trip_distance") > 0))


df_filter.show()

# df_source_transform = (
#     df_source
#         .withColumn(
#             "year",
#             f.year(f.to_date("tpep_pickup_datetime"))
#         )
#         .withColumn(
#             "month",
#             f.date_format("tpep_pickup_datetime", "MM")
#         )
# )

# #######################################################################
# WRITE DATA ############################################################

# save clean data

# (
#     df_source_transform.write
#         .mode("overwrite")
#         .partitionBy("year", "month")
#         .format("parquet")
#         .save(f"{data_root_path}/bronze/{topic}")
# )

logger.info(f"total time process: {datetime.now() - start_time}")

spark.stop()