# Databricks notebook source
# ============================================================
# GOLD - GOLD_EVENTOS
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.eventos"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# ============================================================

df_gold_eventos = spark.sql("""
SELECT 
    de.sk_evento,
    de.nk_evento,
    de.data_evento,
    dt.sk_data,
    dte.sk_tipo_evento,
    dte.descricao_tipo_evento,
    org.sk_orgao,
    org.sigla_orgao,
    org.nome_orgao,
    de.situacao_evento,
    de.descricao_evento,
    dt.ano,
    dt.mes,
    dt.semana_ano
FROM desafio_final_t2.silver.dim_evento de

INNER JOIN desafio_final_t2.silver.bridge_evento_orgao beo 
    ON de.sk_evento = beo.sk_evento

INNER JOIN desafio_final_t2.silver.dim_orgao org 
    ON beo.sk_orgao = org.sk_orgao

INNER JOIN desafio_final_t2.silver.dim_tipo_evento dte 
    ON de.sk_tipo_evento = dte.sk_tipo_evento

INNER JOIN desafio_final_t2.silver.dim_data dt 
    ON de.data_evento = dt.data
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_eventos = (
    df_gold_eventos
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    df_gold_eventos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)


