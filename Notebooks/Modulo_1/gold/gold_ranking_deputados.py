# Databricks notebook source
# ============================================================
# GOLD - Gold_ranking_deputados
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.gold_ranking_deputados"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# ============================================================

df_gold_ranking_deputados_frentes = spark.sql("""
SELECT
    nk_deputado,
    nome_deputado,
    sigla_partido,
    sigla_uf,
    COUNT(DISTINCT nk_frente) AS qtd_frentes,
    COLLECT_SET(titulo_frente) AS frentes_participa
FROM desafio_final_t2.gold.gold_frentes_membros
GROUP BY
    nk_deputado,
    nome_deputado,
    sigla_partido,
    sigla_uf
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_ranking_deputados_frentes = (
    df_gold_ranking_deputados_frentes
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    df_gold_ranking_deputados_frentes.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)

