# Databricks notebook source
# ============================================================
#  SILVER - DIM_TIPO_EVENTO
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_ORIGEM = "desafio_final_t2.bronze.bronze_tipo_evento"
TABELA_DESTINO = "desafio_final_t2.silver.dim_tipo_evento"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA BRONZE
# ============================================================

df_bronze = spark.table(TABELA_ORIGEM)


# ============================================================
# 3. TRANSFORMAÇÃO SILVER
# ============================================================

df_silver = (
    df_bronze
    .select(
        F.col("cod").cast("long").alias("cod_tipo_evento"),
        F.col("sigla").cast("string").alias("sigla_tipo_evento"),
        F.col("nome").cast("string").alias("nome_tipo_evento"),
        F.col("descricao").cast("string").alias("descricao_tipo_evento"),
        F.col("raw_payload").cast("string").alias("raw_payload"),
        F.col("ingested_at").cast("timestamp").alias("bronze_ingested_at")
    )
    .dropDuplicates(["cod_tipo_evento"])
)


window_sk = Window.orderBy("cod_tipo_evento")

# ============================================================
# 4. SURROGATE KEY
# ============================================================

window_sk = Window.orderBy("cod_tipo_evento")

df_dim_tipo_evento = (
    df_silver
    .withColumn("sk_tipo_evento", F.row_number().over(window_sk))
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .withColumn("descricao_tipo_evento",
        F.coalesce(
            F.col("descricao_tipo_evento"),
            F.col("nome_tipo_evento"),
            F.col("sigla_tipo_evento")
        )
    .select(
        "sk_tipo_evento",
        "cod_tipo_evento",
        "sigla_tipo_evento",
        "nome_tipo_evento",
        "descricao_tipo_evento",
        "raw_payload",
        "bronze_ingested_at",
        "updated_at",
        "pipeline_version"
    )
)


# ============================================================
# 6. ESCRITA DELTA SILVER
# ============================================================

(
    df_dim_tipo_evento.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)

