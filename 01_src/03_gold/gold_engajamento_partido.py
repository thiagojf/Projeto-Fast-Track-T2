# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

# ============================================================
# GOLD - Engajamento por Partido
# Consolidar métricas de engajamento dos deputados por partido,
# gerando indicadores agregados para análises políticas e
# dashboards executivos.
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.gold_engajamento_partido"
PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# Agrega métricas de engajamento dos deputados
# por partido político
# ============================================================
df_gold_engajamento_deputado = spark.sql("""
WITH base AS (

    SELECT
        sigla_partido,
        COUNT(*) AS qtd_deputados,
        AVG(score_engajamento) AS media_engajamento,
        MAX(score_engajamento) AS maior_score,
        MIN(score_engajamento) AS menor_score
    FROM desafio_final_t2.gold.gold_engajamento_deputado
    GROUP BY sigla_partido

)

SELECT
    sigla_partido,
    qtd_deputados,
    ROUND(media_engajamento,2) AS media_engajamento,
    ROUND(maior_score,2) AS maior_score,
    ROUND(menor_score,2) AS menor_score,

    DENSE_RANK() OVER (
        ORDER BY media_engajamento DESC
    ) AS ranking_engajamento,

    CASE
        WHEN media_engajamento >= 80 THEN 'Alto'
        WHEN media_engajamento >= 60 THEN 'Médio'
        ELSE 'Baixo'
    END AS faixa_engajamento

FROM base
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_engajamento_partido = (
    df_gold_engajamento_partido
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    df_gold_engajamento_partido.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)