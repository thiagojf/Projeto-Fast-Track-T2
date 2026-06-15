# Databricks notebook source
# ============================================================
# GOLD - gold_ranking_deputados_frentes
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.gold_ihh_frentes"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# ============================================================

df_gold_ihh_frentes  = spark.sql("""
WITH cte_frente_partido AS (

    SELECT
        nk_frente,
        titulo_frente,
        sigla_partido,
        COUNT(DISTINCT nk_deputado) AS qtd_deputados_partido
    FROM desafio_final_t2.gold.gold_frentes_membros
    GROUP BY
        nk_frente,
        titulo_frente,
        sigla_partido

),

cte_total_frente AS (

    SELECT
        nk_frente,
        titulo_frente,
        SUM(qtd_deputados_partido) AS qtd_deputados_frente,
        COUNT(DISTINCT sigla_partido) AS qtd_partidos
    FROM cte_frente_partido
    GROUP BY
        nk_frente,
        titulo_frente

),

cte_proporcao AS (

    SELECT
        fp.nk_frente,
        fp.titulo_frente,
        fp.sigla_partido,
        fp.qtd_deputados_partido,
        tf.qtd_deputados_frente,
        tf.qtd_partidos,
        fp.qtd_deputados_partido / tf.qtd_deputados_frente AS proporcao_partido
    FROM cte_frente_partido fp
    INNER JOIN cte_total_frente tf
        ON fp.nk_frente = tf.nk_frente

),

cte_ihh  AS (
SELECT
    nk_frente,
    titulo_frente,
    qtd_deputados_frente,
    qtd_partidos,
    ROUND(SUM(POWER(proporcao_partido, 2)), 4) AS ihh
FROM cte_proporcao
GROUP BY
    nk_frente,
    titulo_frente,
    qtd_deputados_frente,
    qtd_partidos
)

SELECT
    nk_frente,
    titulo_frente,
    qtd_deputados_frente,
    qtd_partidos,
    ihh,
    CASE
        WHEN ihh <= 0.15 THEN 'Alta diversidade'
        WHEN ihh > 0.15 AND ihh <= 0.25 THEN 'Média diversidade'
        ELSE 'Baixa diversidade'
    END AS classificacao_diversidade
FROM cte_ihh
ORDER BY ihh ASC
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_ranking_deputados_frentes = (
    df_gold_ihh_frentes 
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

