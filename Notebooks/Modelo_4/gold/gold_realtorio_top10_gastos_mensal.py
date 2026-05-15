# Databricks notebook source
# ============================================================
# GOLD - GOLD_TOP10_GASTOS_PARTIDO
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.top_gastos_partido"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# ============================================================

df_gold_top_gastos_partidos= spark.sql("""
WITH gastos_partido AS (
    SELECT
        ano,
        mes,
        ano_mes,
        sigla_partido,
        COUNT(DISTINCT nk_deputado) AS qtd_deputados,
        COUNT(*) AS qtd_despesas,
        ROUND(SUM(valor_liquido), 2) AS valor_total_liquido
    FROM desafio_final_t2.gold.fat_despesas
    GROUP BY
        ano,
        mes,
        ano_mes,
        sigla_partido
),

ranking_partido AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ano_mes
            ORDER BY valor_total_liquido DESC
        ) AS ranking_gasto_partido
    FROM gastos_partido
)

SELECT *
FROM ranking_partido
WHERE ranking_gasto_partido <= 10
ORDER BY
    ano_mes,
    ranking_gasto_partido
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_top_gastos_partidos = (
    df_gold_top_gastos_partidos
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    df_gold_top_gastos_partidos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)


