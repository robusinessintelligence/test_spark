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


########################################################################
# READ DATA ############################################################
df_orders = (
    spark.read
        .format("parquet")
        .load(f"{data_root_path}/raw_data/orders")
)

df_customers = (
    spark.read
        .format("parquet")
        .load(f"{data_root_path}/output/dim_customer")
)

df_orders = df_orders.alias("order")
df_customers = df_customers.alias("customer")

###########################################################################
# PROCESS DATA ############################################################

deduplicate_window = Window.partitionBy("order_id").orderBy(f.col("order_date").desc())

df_deduplicate_register = df_orders.withColumn("duplicate_regs", f.row_number().over(deduplicate_window))

df_deduplicate_orders = (
    df_deduplicate_register
        .filter(f.col("duplicate_regs") == 1)
        .drop("duplicate_regs")
)

df_orders_join_customers = df_deduplicate_orders.join(
    df_customers,
    on=(f.col("order.customer_id") == f.col("customer.customer_id")),
    how="left",
)

df_fact_orders = df_orders_join_customers.select(
    f.col("order.order_id"),
    f.col("order.amount"),
    f.col("order.currency"),
    f.col("order.status"),
    f.col("order.order_date"),
    f.col("order.customer_id"),
    f.col('customer.name'),
    f.col('customer.email'),
    f.col('customer.country'),
    f.col('customer.created_at'),
    f.col("order.processing_date"),
)


# #######################################################################
# WRITE DATA ############################################################

(
    df_fact_orders.write
        .mode("overwrite")
        .partitionBy("processing_date")
        .format("parquet")
        .save(f"{data_root_path}/output/fact_orders")
)

spark.stop()