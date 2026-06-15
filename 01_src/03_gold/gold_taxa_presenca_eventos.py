# Databricks notebook source
# ============================================================
# GOLD - GOLD_DEPUTADOS_TAXA_PRESENCA_EVENTO
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.taxa_presenca_evento"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# ============================================================

df_gold_taxa_presenca_evento = spark.sql("""
WITH deputados_eventos_participados AS (

    SELECT
        dep.nk_deputado,
        dep.nome_deputado,
        dep.sigla_partido,
        dep.sigla_uf,
        de.ano_evento,
        tp.sk_tipo_evento,
        tp.nome_tipo_evento,
        COUNT(DISTINCT ev.id_evento) AS qtd_eventos_participados
    FROM desafio_final_t2.bronze.bronze_eventos_presenca ev
    INNER JOIN desafio_final_t2.silver.dim_evento de
        ON de.nk_evento = ev.id_evento
    INNER JOIN desafio_final_t2.silver.dim_tipo_evento tp
        ON tp.sk_tipo_evento = de.sk_tipo_evento
    INNER JOIN desafio_final_t2.silver.dim_deputado dep
        ON dep.nk_deputado = ev.id
       AND dep.is_current = true
    GROUP BY
        dep.nk_deputado,
        dep.nome_deputado,
        dep.sigla_partido,
        dep.sigla_uf,
        de.ano_evento,
        tp.sk_tipo_evento,
        tp.nome_tipo_evento

),

total_eventos_tipo AS (

    SELECT
        de.ano_evento,
        de.sk_tipo_evento,
        COUNT(DISTINCT de.nk_evento) AS qtd_total_eventos_tipo
    FROM desafio_final_t2.silver.dim_evento de
    GROUP BY
        de.ano_evento,
        de.sk_tipo_evento

)

SELECT
    p.nk_deputado,
    p.nome_deputado,
    p.sigla_partido,
    p.sigla_uf,
    p.ano_evento,
    p.sk_tipo_evento,
    p.nome_tipo_evento,
    p.qtd_eventos_participados,
    t.qtd_total_eventos_tipo,
    ROUND(
        p.qtd_eventos_participados / t.qtd_total_eventos_tipo,
        4
    ) AS taxa_participacao,
    ROUND(
    (
        p.qtd_eventos_participados / t.qtd_total_eventos_tipo
    ) * 100,
    2
) AS percentual_taxa_participacao
FROM deputados_eventos_participados p
INNER JOIN total_eventos_tipo t
    ON p.ano_evento = t.ano_evento
   AND p.sk_tipo_evento = t.sk_tipo_evento
ORDER BY
    taxa_participacao DESC
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_taxa_presenca_evento = (
    df_gold_taxa_presenca_evento
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    df_gold_taxa_presenca_evento.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)

