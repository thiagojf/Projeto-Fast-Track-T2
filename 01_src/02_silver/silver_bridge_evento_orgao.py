# Databricks notebook source
# ============================================================
# SILVER - BRIDGE_EVENTO_ORGAO
# relacionamento N:N apra evitar muitos-para-muitos precisa de tabela ponte (bridge table)
# ============================================================

from pyspark.sql import functions as F

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DIM_ORGAO= "desafio_final_t2.silver.dim_orgao"
TABELA_DIM_EVENTO= "desafio_final_t2.silver.dim_evento"

TABELA_DESTINO = "desafio_final_t2.silver.bridge_evento_orgao"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA DAS TABELAS
# ============================================================

df_silver_dim_orgao = spark.table(TABELA_DIM_ORGAO)
df_silver_dim_evento = spark.table(TABELA_DIM_EVENTO)

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

df_silver_dim_evento = (
    df_silver_dim_evento
    .withColumn(
        "orgaos",
        F.from_json(
            F.col("orgaos"),
            schema_orgaos
        )
    )
)

# ============================================================
# 3. TRANSFORMAÇÃO BASE
# ============================================================

df_silver_evento_explodido = (
    df_silver_dim_evento
    .withColumn("orgao", F.explode("orgaos"))
    .select(
        F.col("sk_evento"),
        F.col("nk_evento"),
        F.col("orgao.id").cast("long").alias("nk_orgao")
    )
    .dropDuplicates(["nk_evento", "nk_orgao"])
)

df_evento_orgao = (
    df_silver_evento_explodido.alias("ex")
    .join(
        df_silver_dim_orgao.select("sk_orgao", "nk_orgao").alias("do"),
        F.col("ex.nk_orgao") == F.col("do.nk_orgao"),
        "left"
    )
    .select(
        F.col("ex.sk_evento"),
        F.col("do.sk_orgao"),
        F.col("ex.nk_evento"),
        F.col("ex.nk_orgao")
    )
    .dropDuplicates(["sk_evento", "sk_orgao"])
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)

# ============================================================
# 4. ESCRITA DELTA SILVER
# ============================================================

(
    df_evento_orgao.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)

