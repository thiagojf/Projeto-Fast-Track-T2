# Databricks notebook source
# ============================================================
# GOLD - GOLD_FRENTES_MEMBROS
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.gold_frentes_membros"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# ============================================================

df_gold_frentes_membros = spark.sql("""
SELECT
    bfd.sk_frente,
    bfd.sk_deputado,
    bfd.nk_frente,
    bfd.nk_deputado,
    sf.titulo_frente,
    sf.id_legislatura,
    d.nome_deputado,
    d.sigla_partido,
    d.sigla_uf
FROM desafio_final_t2.silver.bridge_frente_deputado bfd
INNER JOIN desafio_final_t2.silver.dim_frente sf
    ON sf.sk_frente = bfd.sk_frente
INNER JOIN desafio_final_t2.silver.dim_deputado d
    ON d.sk_deputado = bfd.sk_deputado
WHERE d.is_current = true
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_frentes_membros = (
    df_gold_frentes_membros
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    df_gold_frentes_membros.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)

