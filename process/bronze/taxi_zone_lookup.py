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


# GET ARGS
_PARAMS = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}
logger.info(f"\n _PARAMS: {_PARAMS} \n")


start_time = datetime.now()

########################################################################
# READ DATA ############################################################

topic = "taxi_zone_lookup"
landing_path = f"{data_root_path}/landing/{topic}"

path = f"{landing_path}/taxi_zone_lookup.csv" 

try:

    df_source = (
        spark.read
            .format("csv")
            .option("header", "True")
            .option("inferSchema", "True")
            .option("sep", ",")
            .load(path)
    )

except Exception as err:
    logger.info(f"error on reading file")
    raise err


###########################################################################
# PROCESS DATA ############################################################

df_source = df_source.withColumn("processing_date", f.current_date())

# #######################################################################
# WRITE DATA ############################################################

# save clean data

(
    df_source.write
        .mode("overwrite")
        .format("parquet")
        .save(f"{data_root_path}/bronze/{topic}")
)

logger.info(f"total time process: {datetime.now() - start_time}")

spark.stop()