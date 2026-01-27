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


# GET PROCESS DATE IF EXISTS
if _PARAMS.get("_PROCESS_DATE", None):
    _PROCESS_DATE_STR = _PARAMS["_PROCESS_DATE"]
    _PROCESS_DATE_COL = f.to_date(f.lit(_PARAMS["_PROCESS_DATE"]))

else:
    _PROCESS_DATE_STR = datetime.now().date()
    _PROCESS_DATE_COL = f.current_date()


start_time = datetime.now()

# PROCESS DATA

df_customers = (
    spark.read
        .format("csv")
        .option("header", "True")
        .option("inferSchema", "True")
        .option("sep", ",")
        .load(f"{data_root_path}/input/customers/{_PROCESS_DATE_STR}/customers.csv")
)
df_customers = df_customers.withColumn("processing_date", _PROCESS_DATE_COL)


# handle with duplicated ids
deduplicate_window = Window.partitionBy("customer_id").orderBy(f.col("created_at").desc())

df_customers_deduplicated = (
    df_customers
        .withColumn("duplicate_regs", f.row_number().over(deduplicate_window))
)

# get duplicated ids
df_duplicated_ids = df_customers_deduplicated.where("duplicate_regs > 1")


# handle with invalid regs
check_null_columns = df_customers_deduplicated.columns
find_null_values = " OR ".join([f'{col} IS NULL' for col in check_null_columns])

# get null values
df_invalid_regs = df_customers_deduplicated.filter(find_null_values)


# cleaned data
df_customers_cleaned = (
    df_customers_deduplicated
        .where("duplicate_regs = 1")
        .join(
            df_invalid_regs,
            on="customer_id",
            how="left_anti"
        )
        .drop("duplicate_regs")
)

# df_customers_cleaned = (
#     df_customers_deduplicated
#         .where("duplicate_regs = 1")
#         .na.drop(subset=["name", "country"])
#         .drop("duplicate_regs")
# )

################################################################################
# save duplicated
df_duplicated_ids = (
    df_duplicated_ids
        .withColumn("error_timestamp", f.current_timestamp())
        .withColumn("error_reason", f.lit("Duplicated regs"))

)

(
    df_duplicated_ids.write
        .mode("overwrite")
        .partitionBy("processing_date")
        .option("header", "true")
        .format("csv")
        .save(f"{data_root_path}/rejected_data/customers/duplicated_ids")
)

################################################################################
# save invalid regs
df_invalid_regs = (
    df_invalid_regs
        .withColumn("error_timestamp", f.current_timestamp())
        .withColumn("error_reason", f.lit("Missing data fields"))
)

(
    df_invalid_regs.write
        .mode("overwrite")
        .partitionBy("processing_date")
        .option("header", "true")
        .format("csv")
        .save(f"{data_root_path}/rejected_data/customers/null_fields")
)

################################################################################
# save clean data

(
    df_customers_cleaned.write
        .mode("overwrite")
        .partitionBy("processing_date")
        .format("parquet")
        .save(f"{data_root_path}/raw_data/customers")
)

logger.info(f"total time process: {datetime.now() - start_time}")

spark.stop()