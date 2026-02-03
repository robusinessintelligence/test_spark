import json
import logging
import os
import sys
from datetime import datetime, timedelta

from dateutil.rrule import MONTHLY, rrule
from env_vars import data_root_path
from pyspark.sql import Row, SparkSession
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


# GET ARGS
_PARAMS = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}
logger.info(f"\n _PARAMS: {_PARAMS} \n")


start_time = datetime.now()

########################################################################
# READ DATA ############################################################

vendor_data = [
    Row(vendor_id=1, vendor_name="Creative Mobile Technologies, LLC"),
    Row(vendor_id=2, vendor_name="VeriFone Inc.")
]

# Criando a Dim_RateCode
rate_code_data = [
    Row(rate_code_id=1, rate_code_desc="Standard rate"),
    Row(rate_code_id=2, rate_code_desc="JFK"),
    Row(rate_code_id=3, rate_code_desc="Newark"),
    Row(rate_code_id=4, rate_code_desc="Nassau or Westchester"),
    Row(rate_code_id=5, rate_code_desc="Negotiated fare"),
    Row(rate_code_id=6, rate_code_desc="Group ride")
]

###########################################################################
# PROCESS DATA ############################################################

dim_vendor = spark.createDataFrame(vendor_data)
dim_rate_code = spark.createDataFrame(rate_code_data)

# #######################################################################
# WRITE DATA ############################################################

# save clean data

(
    dim_vendor.write
        .mode("overwrite")
        .format("parquet")
        .save(f"{data_root_path}/silver/dim_vendor")
)

(
    dim_rate_code.write
        .mode("overwrite")
        .format("parquet")
        .save(f"{data_root_path}/silver/dim_rate_code")
)


logger.info(f"total time process: {datetime.now() - start_time}")

spark.stop()