import json
import logging
import os
import sys
from datetime import datetime, timedelta

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
        .appName("load_database")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# SPARK CONFIGS
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# CONFIG SPARK #############################################

start_time = datetime.now()


# CONFIG CONN DB #############################################
db_url = "jdbc:postgresql://db:5432/nyc_taxi"

db_properties = {
    "user": "user_spark",
    "password": "password_spark",
    "driver": "org.postgresql.Driver",
    "batchsize": "10000"
}
# CONFIG CONN DB #############################################

########################################################################
# READ DATA ############################################################

silver_path = f"{data_root_path}/silver"

logger.info(f"reading tables")

try:
    df_dim_rate_code = spark.read.format("parquet").load(f"{silver_path}/dim_rate_code")
    df_dim_vendor = spark.read.format("parquet").load(f"{silver_path}/dim_vendor")
    df_dim_zone = spark.read.format("parquet").load(f"{silver_path}/dim_zone")
    # df_fact_trips = spark.read.format("parquet").load(f"{silver_path}/fact_trips")

except Exception as err:
    logger.info(f"error on reading files")
    raise err

###########################################################################
# PROCESS DATA ############################################################

# #######################################################################
# WRITE DATA ############################################################

# save data to db

logger.info(f"writing tables in db")

try:

    df_dim_rate_code.write.jdbc(db_url, "dim_rate_code", mode="overwrite", properties=db_properties)
    df_dim_vendor.write.jdbc(db_url, "dim_vendor", mode="overwrite", properties=db_properties)
    df_dim_zone.write.jdbc(db_url, "dim_zone", mode="overwrite", properties=db_properties)
    # df_fact_trips.write.jdbc(db_url, "fact_trips", mode="overwrite", properties=db_properties)

except Exception as err:
    logger.info(f"error on write to db")
    raise err

logger.info(f"total time process: {datetime.now() - start_time}")

spark.stop()
