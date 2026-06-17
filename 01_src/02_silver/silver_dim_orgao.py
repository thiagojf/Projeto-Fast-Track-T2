# Databricks notebook source
# ============================================================
#  SILVER - DIM_ORGAO
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import explode

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_ORIGEM = "desafio_final_t2.silver.dim_evento"
TABELA_DESTINO = "desafio_final_t2.silver.dim_orgao"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA BRONZE
# ============================================================

df_bronze = spark.table(TABELA_ORIGEM)

# ============================================================
# 2.1 RECONSTRUÇÃO DE ESTRUTURAS JSON
# ============================================================

schema_orgaos = """
array<
    struct<
        id:bigint,
        uri:string,
        sigla:string,
        nome:string,
        apelido:string,
        codTipoOrgao:int,
        tipoOrgao:string,
        nomePublicacao:string,
        nomeResumido:string
    >
>
"""

df_bronze = (
    df_bronze
    .withColumn(
        "orgaos",
        F.from_json(
            F.col("orgaos"),
            schema_orgaos
        )
    )
)

# ============================================================
# 3. TRANSFORMAÇÃO SILVER
# ============================================================

df_silver_evento_explodido = (
    df_bronze
    .withColumn("orgao", F.explode("orgaos"))
    .select(
        F.col("orgao.id").cast("long").alias("nk_orgao"),
        F.col("orgao.sigla").cast("string").alias("sigla_orgao"),
        F.col("orgao.nome").cast("string").alias("nome_orgao"),
        F.col("orgao.apelido").cast("string").alias("apelido_orgao"),
        F.col("orgao.codTipoOrgao").cast("long").alias("cod_tipo_orgao"),
        F.col("orgao.tipoOrgao").cast("string").alias("tipo_orgao"),
        F.col("orgao.nomePublicacao").cast("string").alias("nome_publicacao"),
        F.col("orgao.nomeResumido").cast("string").alias("nome_resumido")
    )
    .dropDuplicates(["nk_orgao"])
)

# ============================================================
# 4. SURROGATE KEY
# ============================================================

window_sk = Window.orderBy("nk_orgao")

df_dim_orgaos = (
    df_silver_evento_explodido
    .withColumn("sk_orgao", F.row_number().over(window_sk))
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .select(
        "sk_orgao",
        "nk_orgao",
        "sigla_orgao",
        "nome_orgao",
        "apelido_orgao",
        "cod_tipo_orgao",
        "tipo_orgao",
        "nome_publicacao",
        "nome_resumido",
        "updated_at",
        "pipeline_version"
    )
)


# ============================================================
# 5. ESCRITA DELTA SILVER
# ============================================================

(
    df_dim_orgaos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)

