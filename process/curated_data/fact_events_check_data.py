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

df_events = (
    spark.read
        .format("parquet")
        .load(f"{data_root_path}/raw_data/events")
)

df_events.show()
