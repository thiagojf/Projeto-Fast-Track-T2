# Databricks notebook source
# ============================================================
# GOLD - DETALHES CPI
# Projeto Final - Engenharia de Dados
# Camada Gold | Disponibiliza informações consolidadas sobre Comissões Parlamentares de Inquérito (CPIs) para consumo analítico.
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.gold_gold_cpis"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# Consolida informações da dimensão CPI para disponibilização em camada analítica
# ============================================================
df_gold_cpis = spark.sql("""
SELECT
    nk_cpi,
    nome_cpi,
    sigla,
    tipo_orgao,
    status_cpi,
    data_inicio,
    data_instalacao,
    data_fim,
    data_fim_original,
    url_website,
    uri_orgao,
    CASE
        WHEN data_inicio IS NOT NULL THEN 'Sim'
        ELSE 'Não'
    END AS possui_data_inicio,

    CASE
        WHEN data_fim IS NOT NULL THEN 'Sim'
        ELSE 'Não'
    END AS possui_data_fim

FROM desafio_final_t2.silver.silver_dim_cpi
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_cpis = (
    df_gold_cpis
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    df_gold_cpis.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)