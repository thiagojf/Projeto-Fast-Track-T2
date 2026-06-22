# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/99_utils/common_utils

# COMMAND ----------

# ============================================================
# SILVER - BRIDGE_FRENTE_DEPUTADO
# deputado ↔ frente é um relacionamento N:N apra evitar muitos-para-muitos precisa de tabela ponte (bridge table)
# ============================================================

from pyspark.sql import functions as F

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_BRONZE_MEMBROS = "desafio_final_t2.bronze.bronze_frentes_membros"
TABELA_DIM_FRENTE = "desafio_final_t2.silver.dim_frente"
TABELA_DIM_DEPUTADO = "desafio_final_t2.silver.dim_deputado"

TABELA_DESTINO = "desafio_final_t2.silver.bridge_frente_deputado"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA DAS TABELAS
# ============================================================

df_membros = spark.table(TABELA_BRONZE_MEMBROS)
df_dim_frente = spark.table(TABELA_DIM_FRENTE)
df_dim_deputado = spark.table(TABELA_DIM_DEPUTADO)

# ============================================================
# 3. NORMALIZAÇÃO DA BRONZE
# ============================================================

df_membros_base = (
    df_membros
    .select(
        F.col("id_frente").cast("long").alias("nk_frente"),
        F.col("id").cast("long").alias("nk_deputado"),
        F.col("nome").cast("string").alias("nome_deputado_bronze"),
        F.col("siglaPartido").cast("string").alias("sigla_partido_bronze"),
        F.col("siglaUf").cast("string").alias("sigla_uf_bronze"),
        F.col("idLegislatura").cast("int").alias("id_legislatura"),
        F.col("ingested_at").cast("timestamp").alias("bronze_ingested_at")
    )
    .dropDuplicates(["nk_frente", "nk_deputado"])
)

# ============================================================
# 4. JOIN COM DIMENSÕES
# ============================================================

df_bridge = (
    df_membros_base.alias("m")

    .join(
        df_dim_frente.alias("f"),
        F.col("m.nk_frente") == F.col("f.nk_frente"),
        "left"
    )

    .join(
        df_dim_deputado
        .filter(F.col("is_current") == True)
        .alias("d"),
        F.col("m.nk_deputado") == F.col("d.nk_deputado"),
        "left"
    )

    .select(
        F.col("f.sk_frente"),
        F.col("d.sk_deputado"),
        F.col("m.nk_frente"),
        F.col("m.nk_deputado"),
        F.coalesce(F.col("d.nome_deputado"), F.col("m.nome_deputado_bronze")).alias("nome_deputado"),
        F.coalesce(F.col("d.sigla_partido"), F.col("m.sigla_partido_bronze")).alias("sigla_partido"),
        F.coalesce(F.col("d.sigla_uf"), F.col("m.sigla_uf_bronze")).alias("sigla_uf"),
        F.col("m.id_legislatura"),
        F.col("m.bronze_ingested_at")
    )

    .dropDuplicates(["nk_frente", "nk_deputado"])
)

# ============================================================
# 5. AUDITORIA
# ============================================================

df_bridge = (
    df_bridge
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)

# ============================================================
# 6. ESCRITA DELTA SILVER
# ============================================================

(
    df_bridge.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)
