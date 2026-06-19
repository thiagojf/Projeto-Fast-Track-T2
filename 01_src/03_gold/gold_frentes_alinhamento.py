# Databricks notebook source
# ============================================================
# GOLD - GOLD FRENTES ALINHAMENTO
# Avaliar o grau de alinhamento dos membros de cada Frente Parlamentar
# nas votações da Câmara dos Deputados.
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.gold_frentes_membros"
PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# Consolida métricas de alinhamento entre membros
# das Frentes Parlamentares em votações analisadas
# ============================================================

gold_frentes_alinhamento = spark.sql("""
SELECT
    nk_frente,
    titulo_frente,
    COUNT(*) AS qtd_votacoes,
    ROUND(AVG(percent_alinhamento),2) AS media_alinhamento,
    MIN(percent_alinhamento) AS menor_alinhamento,
    MAX(percent_alinhamento) AS maior_alinhamento,
    CASE
        WHEN media_alinhamento >= 80 THEN 'Muito Coesa'
        WHEN media_alinhamento >= 60 THEN 'Coesa'
        ELSE 'Fragmentada'
    END as classificacao,
    ROUND(STDDEV(percent_alinhamento),2) AS desvio_alinhamento,
    CASE
        WHEN STDDEV(percent_alinhamento) < 10
        THEN 'Sim'
    ELSE 'Não'
    END as possui_baixo_desvio
FROM desafio_final_t2.gold.gold_frentes_votacoes
GROUP BY
    nk_frente,
    titulo_frente
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

gold_frentes_alinhamento = (
    gold_frentes_alinhamento
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    gold_frentes_alinhamento.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)

