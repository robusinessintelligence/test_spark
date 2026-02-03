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


# _START_DATE ########################################################################
if _PARAMS.get("_START_DATE", None):
    try:
        _START_DATE = datetime.strptime(_PARAMS["_START_DATE"], '%Y-%m-%d')
        star_year_month = _START_DATE.strftime("%Y-%m")

    except Exception as err:
        logger.error(f"Error on get _START_DATE param")
        raise err

else:
    _START_DATE = datetime.now().date()
    star_year_month = _START_DATE.strftime("%Y-%m")


# _END_DATE ########################################################################
if _PARAMS.get("_END_DATE", None):
    try:
        _END_DATE = datetime.strptime(_PARAMS["_END_DATE"], '%Y-%m-%d')
        end_year_month = _END_DATE.strftime("%Y-%m")

    except Exception as err:
        logger.error(f"Error on get _END_DATE param")
        raise err

else:
    _END_DATE = datetime.now().date()
    end_year_month = _END_DATE.strftime("%Y-%m")

# range of months
months_list = [
    dt.strftime("%Y-%m") 
    for dt in rrule(MONTHLY, dtstart=_START_DATE, until=_END_DATE)
]

logger.info(f"getting data from months: {months_list}")

start_time = datetime.now()


########################################################################
# READ DATA ############################################################

topic = "yellow_taxi_trip"
landing_path = f"{data_root_path}/landing/{topic}"

paths = [
    f"{landing_path}/{month}/yellow_tripdata_{month}.parquet" 
    for month in months_list
]

logger.info(f"getting paths: {paths}")

try:

    df_source = (
        spark.read
            .format("parquet")
            .load(paths)
    )

except Exception as err:
    logger.info(f"error on reading file")
    raise err


###########################################################################
# PROCESS DATA ############################################################

df_source = df_source.withColumn("processing_date", f.current_date())

df_source_transform = (
    df_source
        .withColumn(
            "year",
            f.year(f.to_date("tpep_pickup_datetime"))
        )
        .withColumn(
            "month",
            f.date_format("tpep_pickup_datetime", "MM")
        )
)

# #######################################################################
# WRITE DATA ############################################################

# save clean data

(
    df_source_transform.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .format("parquet")
        .save(f"{data_root_path}/bronze/{topic}")
)

logger.info(f"total time process: {datetime.now() - start_time}")

spark.stop()