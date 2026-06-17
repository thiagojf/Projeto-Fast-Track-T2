# Databricks notebook source
# Carrega configuracoes globais e funcoes utilitarias

# COMMAND ----------

# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

# ============================================================
#  SILVER - DIM_DESPESAS
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import explode

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_ORIGEM = "desafio_final_t2.bronze.bronze_despesas"
TABELA_DESTINO = "desafio_final_t2.silver.despesas"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA BRONZE
# ============================================================

df_bronze = spark.table(TABELA_ORIGEM)



df_dim_despesas = (
    df_bronze
    .select(
            F.col("nk_deputado").cast("integer").alias("nk_deputado"),
            F.col("ano").cast("integer").alias("ano"),
            F.col("mes").cast("integer").alias("mes"),
            F.concat_ws("-", F.col("ano"), F.lpad(F.col("mes"), 2, "0")).alias("ano_mes"),
            F.col("tipoDespesa").cast("string").alias("tipo_despesa"),
            F.col("codDocumento").cast("long").alias("cod_documento"),
            F.col("tipoDocumento").cast("string").alias("tipo_documento"),
            F.col("codTipoDocumento").cast("integer").alias("cod_tipo_documento"),
            F.to_date("dataDocumento").alias("data_documento"),
            F.col("numDocumento").cast("string").alias("num_documento"),
            F.col("valorDocumento").cast("double").alias("valor_documento"),
            F.col("valorLiquido").cast("double").alias("valor_liquido"),
            F.col("valorGlosa").cast("double").alias("valor_glosa"),
            F.col("nomeFornecedor").cast("string").alias("nome_fornecedor"),
            F.col("cnpjCpfFornecedor").cast("string").alias("cnpj_cpf_fornecedor"),
            F.regexp_replace(F.col("cnpjCpfFornecedor"), "[^0-9]", "").alias("cnpj_cpf_fornecedor_limpo"),
            F.col("urlDocumento").cast("string").alias("url_documento"),
            F.col("codLote").cast("long").alias("cod_lote"),
            F.col("parcela").cast("integer").alias("parcela"),
            F.col("updated_at").cast("timestamp").alias("updated_at"),
            F.col("pipeline_version").cast("string").alias("pipeline_version")
    )
    .dropDuplicates([
            "nk_deputado",
            "cod_documento",
            "cod_lote",
            "parcela"
    ])
)

# ============================================================
# 5. CAMPOS DERIVADOS DE DATA
# ============================================================

df_dim_despesas = (
    df_dim_despesas
    .withColumn("data_despesa", F.to_date("data_documento"))
    .withColumn("ano_despesa", F.year("data_documento"))
    .withColumn("mes_despesa", F.month("data_documento"))
    .withColumn("semana_despesa", F.weekofyear("data_documento"))
)


# ============================================================
# 6. SURROGATE KEY
# ============================================================

df_dim_despesas = (
    df_dim_despesas
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .withColumn("nk_despesa",sha2(concat_ws("|",F.col("nk_deputado"),F.col("cnpj_cpf_fornecedor"),F.col("data_documento"),F.col("valor_liquido"),F.col("cod_documento"),F.col("num_documento"),F.col("cod_lote")),256))
    .select(
            "nk_despesa",
            "nk_deputado",
            "ano",
            "mes",
            "ano_mes",
            "tipo_despesa",
            "cod_documento",
            "tipo_documento",
            "cod_tipo_documento",
            "data_documento",
            "num_documento",
            "valor_documento",
            "valor_liquido",
            "valor_glosa",
            "nome_fornecedor",
            "cnpj_cpf_fornecedor",
            "cnpj_cpf_fornecedor_limpo",
            "url_documento",
            "cod_lote",
            "parcela",
            "data_despesa",
            "ano_despesa",
            "mes_despesa",
            "semana_despesa",
            "updated_at",
            "pipeline_version"
    )
)


# ============================================================
# 7. ESCRITA DELTA SILVER
# Alterado a estrutura para merge, considerando a necessidade de manter o histórico dos deputados permitindo
# atualização e inserção de registros sem recriação completa das tabelas.
# ============================================================

executar_merge(
    df=df_dim_despesas,
    tabela_destino=TABELA_DESTINO,
    condicao_merge="""
        dest.nk_despesa = orig.nk_despesa
    """
)
