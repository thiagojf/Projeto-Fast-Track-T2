# Databricks notebook source
# ============================================================
#  SILVER - DIM_CATEGORIA_DESPESA
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import explode

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_ORIGEM = "desafio_final_t2.silver.despesas"
TABELA_DESTINO = "desafio_final_t2.silver.dim_categoria_despesa"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA BRONZE
# ============================================================

df_bronze = spark.table(TABELA_ORIGEM)


# ============================================================
# 3. TRANSFORMAÇÃO SILVER
# ============================================================

df_silver_tipo_despesa = (
    df_bronze
    .select(
        F.col("tipo_despesa").cast("string").alias("nk_categoria_despesa")
    )
    .dropDuplicates(["nk_categoria_despesa"])
)

# ============================================================
# 4. SURROGATE KEY
# ============================================================

window_sk = Window.orderBy("nk_categoria_despesa")

df_dim_categoria_despesa = (
    df_silver_tipo_despesa
    .withColumn("sk_categoria_despesa", F.row_number().over(window_sk))
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .select(
        "sk_categoria_despesa",
        "nk_categoria_despesa",
        "updated_at",
        "pipeline_version"
    )
)


# ============================================================
# 5. ESCRITA DELTA SILVER
# ============================================================

(
    df_dim_categoria_despesa.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)

