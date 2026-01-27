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
df_customers = (
    spark.read
        .format("parquet")
        .load(f"{data_root_path}/raw_data/customers")
)

###########################################################################
# PROCESS DATA ############################################################

deduplicate_window = Window.partitionBy("customer_id").orderBy(f.col("processing_date").desc(), f.col("created_at").desc())

df_deduplicate_register = df_customers.withColumn("duplicate_regs", f.row_number().over(deduplicate_window))

df_cleaned_customers = (
    df_deduplicate_register
        .filter(f.col("duplicate_regs") == 1)
        .drop("duplicate_regs")
)


# #######################################################################
# WRITE DATA ############################################################

(
    df_cleaned_customers.write
        .mode("overwrite")
        .partitionBy("processing_date")
        .format("parquet")
        .save(f"{data_root_path}/output/dim_customer")
)

logger.info(f"total time process: {datetime.now() - start_time}")

spark.stop()