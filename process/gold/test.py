from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

# 1. Configuração da Sessão com suporte ao Postgres
spark = SparkSession.builder \
    .appName("NYC Taxi Gold Load") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Configurações de conexão
db_url = "jdbc:postgresql://db:5432/nyc_taxi"
db_properties = {
    "user": "user_spark",
    "password": "password_spark",
    "driver": "org.postgresql.Driver",
    "batchsize": "10000" # Importante para performance com 1.4M de linhas
}

def load_gold_layer():
    # --- CARGA DAS DIMENSÕES ESTÁTICAS ---
    print("Iniciando carga de dimensões...")
    
    # Dim Vendor
    vendor_df = spark.createDataFrame([
        Row(vendor_id=1, vendor_name="Creative Mobile Technologies"),
        Row(vendor_id=2, vendor_name="VeriFone Inc.")
    ])
    vendor_df.write.jdbc(db_url, "dim_vendor", mode="overwrite", properties=db_properties)

    # Dim RateCode
    rate_code_df = spark.createDataFrame([
        Row(rate_code_id=1, rate_code_desc="Standard rate"),
        Row(rate_code_id=2, rate_code_desc="JFK"),
        Row(rate_code_id=3, rate_code_desc="Newark"),
        Row(rate_code_id=4, rate_code_desc="Nassau/Westchester"),
        Row(rate_code_id=5, rate_code_desc="Negotiated fare"),
        Row(rate_code_id=6, rate_code_desc="Group ride")
    ])
    rate_code_df.write.jdbc(db_url, "dim_rate_code", mode="overwrite", properties=db_properties)

    print("Carga concluída com sucesso!")

if __name__ == "__main__":
    load_gold_layer()