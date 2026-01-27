import logging
import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# LOG ################################################################################
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
# LOG ################################################################################

logger = logging.getLogger(__name__)

# 1. Configuração da SparkSession
# 'spark-master' é o nome do serviço no seu docker-compose
spark = (
    SparkSession.builder
        .appName("customers_raw")
        .master("spark://spark-master:7077")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

logger.info(">>> Iniciando processamento de larga escala...")

data = spark.range(0, 1000000).withColumn("valor", F.rand() * 100)

resultado = data.select(F.avg("valor").alias("media_vendas"))

resultado.show()
logger.info(">>> Processamento concluído com sucesso!")

spark.stop()