import logging
import os
import sys
from datetime import datetime, timedelta

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
        .appName("customers_raw")
        .master("spark://spark-master:7077")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
# CONFIG SPARK #############################################

start_time = datetime.now()

# PROCESS DATA

df_orders = (
    spark.read
        .format("json")
        .option("multiline", "true")
        .load("/jobs/input/orders.json")
)


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
        

# ################################################################################
# save null fields
df_null_fields = (
    df_null_fields
        .withColumn("processing_date", f.current_date())
        .withColumn("error_timestamp", f.current_timestamp())
        .withColumn("error_reason", f.lit("null fields regs"))

)

(
    df_null_fields.write
        .mode("overwrite")
        .partitionBy("processing_date")
        .format("csv")
        .save("/jobs/rejected_data/orders/null_fields")
)

################################################################################
# save clean data
df_orders_transform_data = (
    df_orders_transform_data
        .withColumn("processing_date", f.current_date())
)

(
    df_orders_transform_data.write
        .mode("overwrite")
        .partitionBy("processing_date")
        .format("parquet")
        .save("/jobs/raw_data/orders")
)


logger.info(f"total time process: {datetime.now() - start_time}")

spark.stop()