# Databricks notebook source
# ============================================================
# GOLD - GOLD_COMPARATIVO_PERIODO_ELEITORAL
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.comparativo_periodo_eleitoral"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# ============================================================

df_gold_comparativo_periodo_eleitoral = spark.sql("""
    SELECT 
        dt.ano,
        dt.periodo_eleitoral,
        dt.descricao_periodo_eleitoral,
        ev.descricao_tipo_evento,
        COUNT(DISTINCT ev.nk_evento) AS qtd_eventos,
        ROUND(
            COUNT(DISTINCT ev.nk_evento) / COUNT(DISTINCT dt.semana_ano),
            2
        ) AS media_eventos_semana,
        COUNT(DISTINCT ev.sk_orgao) AS qtd_orgaos
    FROM desafio_final_t2.gold.eventos ev
    INNER JOIN desafio_final_t2.silver.dim_data dt
        ON dt.data = ev.data_evento
    GROUP BY
        dt.ano,
        dt.periodo_eleitoral,
        dt.descricao_periodo_eleitoral,
        ev.descricao_tipo_evento
    ORDER BY
        dt.ano,
        ev.descricao_tipo_evento,
        dt.periodo_eleitoral
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_comparativo_periodo_eleitoral = (
    df_gold_comparativo_periodo_eleitoral
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    df_gold_comparativo_periodo_eleitoral.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)

