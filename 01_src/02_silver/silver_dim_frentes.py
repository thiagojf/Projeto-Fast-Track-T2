# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/merge_utils

# COMMAND ----------

# ============================================================
# SILVER - DIM_FRENTE
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_ORIGEM = "desafio_final_t2.bronze.bronze_frentes"
TABELA_DESTINO = "desafio_final_t2.silver.dim_frente"
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
        F.col("id").cast("long").alias("nk_frente"),
        F.col("idLegislatura").cast("int").alias("id_legislatura"),
        F.col("titulo").cast("string").alias("titulo_frente"),
        F.col("uri").cast("string").alias("uri_frente"),
        F.col("ingested_at").cast("timestamp").alias("bronze_ingested_at")
    )
    .dropDuplicates(["nk_frente", "id_legislatura"])
)

# ============================================================
# 4. CRIA SURROGATE KEY
# ============================================================

window_sk = Window.orderBy("nk_frente")

df_dim_frentes = (
    df_silver
    .withColumn("sk_frente", F.row_number().over(window_sk))
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .select(
        "sk_frente",
        "nk_frente",
        "id_legislatura",
        "titulo_frente",
        "uri_frente",
        "bronze_ingested_at",
        "updated_at",
        "pipeline_version"
    )
)

# ============================================================
# 5. ESCRITA DELTA SILVER
# Alterado a estrutura para merge, considerando a necessidade de manter o histórico dos deputados permitindo
# atualização e inserção de registros sem recriação completa das tabelas.
# ============================================================

executar_merge(
    df=df_dim_frentes,
    tabela_destino=TABELA_DESTINO,
    condicao_merge="""
        dest.id_frente = orig.id_frente
    """
)