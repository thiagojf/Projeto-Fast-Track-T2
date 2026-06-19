# Databricks notebook source
# ============================================================
# GOLD - GOLD_DENSIDADE_EVENTOS_SEMANAS
# Projeto Final - Engenharia de Dados
# Camada Gold | Disponibiliza uma visão temporal da distribuição de eventos legislativos por semana. 
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.densidade_eventos_semana"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# ============================================================

df_gold_eventos_semana = spark.sql("""
WITH eventos_semana AS (
    SELECT
        dt.ano,
        dt.semana_ano,
        COUNT(DISTINCT e.nk_evento) AS qtd_eventos
    FROM desafio_final_t2.silver.dim_data dt
    LEFT JOIN desafio_final_t2.silver.dim_evento e
        ON dt.data = e.data_evento
    GROUP BY
        dt.ano,
        dt.semana_ano
)

SELECT
    ano,
    semana_ano,
    CONCAT(ano, '-', LPAD(semana_ano, 2, '0')) AS ano_semana,
    qtd_eventos,
    CASE
        WHEN qtd_eventos > 0 THEN true
        ELSE false
    END AS possui_evento
FROM eventos_semana
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_eventos_semana = (
    df_gold_eventos_semana
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    df_gold_eventos_semana.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)