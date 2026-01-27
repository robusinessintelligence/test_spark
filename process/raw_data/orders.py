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
        .appName("orders_job")
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


# GET PROCESS DATE IF EXISTS
if _PARAMS.get("_PROCESS_DATE", None):
    _PROCESS_DATE_STR = _PARAMS["_PROCESS_DATE"]
    _PROCESS_DATE_COL = f.to_date(f.lit(_PARAMS["_PROCESS_DATE"]))

else:
    _PROCESS_DATE_STR = datetime.now().date()
    _PROCESS_DATE_COL = f.current_date()


start_time = datetime.now()


########################################################################
# READ DATA ############################################################
df_orders = (
    spark.read
        .format("json")
        .option("multiline", "true")
        .load(f"{data_root_path}/input/orders/{_PROCESS_DATE_STR}/orders.json")
)
df_orders = df_orders.withColumn("processing_date", _PROCESS_DATE_COL)


###########################################################################
# PROCESS DATA ############################################################

# handle with null fields
check_null_columns = df_orders.columns
find_null_values = " OR ".join([f'{col} IS NULL' for col in check_null_columns])
df_null_fields = df_orders.filter(find_null_values)


# clean data
df_orders_cleaned = (
    df_orders
        .join(
            df_null_fields,
            on="order_id",
            how="left_anti"
        )
)


# handle with data transformations
df_orders_transform_data = df_orders_cleaned.select(
    f.col("order_id"),
    f.abs(f.col("amount")).alias("amount"),
    f.col("currency"),
    f.col("customer_id"),

    f.coalesce(
        f.try_to_date(f.col("order_date"), "yyyy-MM-dd"),
        f.try_to_date(f.col("order_date"), "dd-MM-yyyy"),
        f.try_to_date(f.col("order_date"), "dd/MM/yyyy"),
    ).alias("order_date"),

    f.upper(f.col("status")).alias("status"),
)
        

# #######################################################################
# WRITE DATA ############################################################
df_null_fields = (
    df_null_fields
        .withColumn("error_timestamp", f.current_timestamp())
        .withColumn("error_reason", f.lit("null fields regs"))

)

(
    df_null_fields.write
        .mode("overwrite")
        .partitionBy("processing_date")
        .option("header", "true")
        .format("csv")
        .save(f"{data_root_path}/rejected_data/orders/null_fields")
)

################################################################################
# save clean data
(
    df_orders_transform_data.write
        .mode("overwrite")
        .partitionBy("processing_date")
        .format("parquet")
        .save(f"{data_root_path}/raw_data/orders")
)


logger.info(f"total time process: {datetime.now() - start_time}")

spark.stop()