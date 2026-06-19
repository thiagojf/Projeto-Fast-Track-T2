# Databricks notebook source
# ============================================================
# GOLD - GOLD_FAT_DESPESAS
# Projeto Final - Engenharia de Dados
# Camada Gold | Diponibiliza a tabela fato de despesas, enriquecendo os dados com as dimensões de deputado, fornecedor e categoria de despesa. 
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.fat_despesas"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# ============================================================

df_gold_fat_despesas= spark.sql("""
SELECT
dpt.sk_deputado,
dpt.sigla_partido,
df.sk_fornecedor,
cd.sk_categoria_despesa,
dpt.nk_deputado,
dp.ano,
dp.mes,
dp.ano_mes,
dp.data_documento,
dp.tipo_documento,
dp.cod_documento,
dp.cod_lote,
dp.parcela,
dp.valor_documento,
dp.valor_liquido,
dp.valor_glosa,
dp.url_documento,
dp.updated_at,
dp.pipeline_version
FROM desafio_final_t2.silver.despesas dp

INNER JOIN desafio_final_t2.silver.dim_deputado dpt
    ON dp.nk_deputado = dpt.nk_deputado
   AND dpt.is_current = true

INNER JOIN desafio_final_t2.silver.dim_fornecedor df
    ON COALESCE(NULLIF(dp.cnpj_cpf_fornecedor_limpo, ''), dp.nome_fornecedor) = df.nk_fornecedor

INNER JOIN desafio_final_t2.silver.dim_categoria_despesa cd
    ON dp.tipo_despesa = cd.nk_categoria_despesa
where dpt.is_current = true
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_fat_despesas = (
    df_gold_fat_despesas
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    df_gold_fat_despesas.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)


