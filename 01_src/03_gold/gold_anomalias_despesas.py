# Databricks notebook source
# ============================================================
# GOLD - ANOMALIAS DESPESAS
# Detecção de padrões e anomalias, foi implementada uma análise estatística baseada em Z-Score segmentada por categoria de despesa e unidade federativa. Dessa forma, # cada despesa é comparada com o comportamento esperado do seu próprio grupo, permitindo identificar gastos atípicos de forma mais justa e contextualizada.
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.anomalias_despesas"
PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# ============================================================

df_gold_anomalias_despesas= spark.sql("""
WITH estatisticas AS (

    SELECT
            dcat.sk_categoria_despesa,
            ddep.sigla_uf,
            AVG(fdesp.valor_liquido) AS media_grupo,
            STDDEV(fdesp.valor_liquido) AS desvio_grupo
    FROM desafio_final_t2.gold.fat_despesas fdesp
        inner join desafio_final_t2.silver.dim_categoria_despesa dcat on fdesp.sk_categoria_despesa = dcat.sk_categoria_despesa
        inner join desafio_final_t2.silver.dim_deputado ddep on fdesp.sk_deputado = ddep.sk_deputado

    GROUP BY
            dcat.sk_categoria_despesa,
            ddep.sigla_uf
)

SELECT
    fd.sk_deputado,
    ddep.nome_deputado,
    ddep.sigla_uf,
    dcat.nk_categoria_despesa as tipo_despesa,
    ROUND(fd.valor_liquido,2) as valor_liquido,
    ROUND(e.media_grupo,2) as media_grupo,
    ROUND(e.desvio_grupo,2) as desvio_grupo,

    ROUND(
        try_divide(
            fd.valor_liquido - e.media_grupo,
            e.desvio_grupo
        ),
        2
    ) AS z_score,

    CASE
        WHEN ABS(z_score) < 1 THEN 'Comportamento esperado'
        WHEN ABS(z_score) >= 1 AND ABS(z_score) < 2 THEN 'Leve desvio'
        WHEN ABS(z_score) >= 2 AND ABS(z_score) < 3 THEN 'Atenção'
        WHEN ABS(z_score) >= 3 AND ABS(z_score) < 4 THEN 'Possível anomalia'
        WHEN ABS(z_score) >= 4 THEN 'Muito atípico'
    END AS classificacao_anomalia

FROM desafio_final_t2.gold.fat_despesas fd
inner join desafio_final_t2.silver.dim_categoria_despesa dcat on fd.sk_categoria_despesa = dcat.sk_categoria_despesa
inner join desafio_final_t2.silver.dim_deputado ddep on fd.sk_deputado = ddep.sk_deputado
INNER JOIN estatisticas e  ON dcat.sk_categoria_despesa = e.sk_categoria_despesa AND ddep.sigla_uf = e.sigla_uf
WHERE e.desvio_grupo > 0
ORDER BY
    z_score DESC;
    
""")

# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_anomalias_despesas = (
    df_gold_anomalias_despesas
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    df_gold_anomalias_despesas.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)
