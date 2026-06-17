# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

# ============================================================
#  SILVER - DIM_FORNECEDOR
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_ORIGEM = "desafio_final_t2.silver.despesas"
TABELA_DESTINO = "desafio_final_t2.silver.dim_fornecedor"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA BRONZE
# ============================================================

df_despesas = spark.table(TABELA_ORIGEM)


# ============================================================
# 3. TRANSFORMAÇÃO SILVER
# ============================================================

df_base_fornecedor = (
    df_despesas
    .select(
        F.col("cnpj_cpf_fornecedor").cast("string").alias("cnpj_cpf_fornecedor"),
        F.col("cnpj_cpf_fornecedor_limpo").cast("string").alias("cnpj_cpf_fornecedor_limpo"),
        F.col("nome_fornecedor").cast("string").alias("nome_fornecedor")
    )
    .withColumn(
        "nk_fornecedor",
        F.coalesce(
            F.when(
                F.trim(F.col("cnpj_cpf_fornecedor_limpo")) != "",
                F.col("cnpj_cpf_fornecedor_limpo")
            ),
            F.col("nome_fornecedor")
        )
    )
    .withColumn(
        "tipo_pessoa",
        F.when(F.length("cnpj_cpf_fornecedor_limpo") == 14, F.lit("PJ"))
         .when(F.length("cnpj_cpf_fornecedor_limpo") == 11, F.lit("PF"))
         .otherwise(F.lit("NAO_IDENTIFICADO"))
    )
    .dropDuplicates(["nk_fornecedor"])
)


# ============================================================
# 4. SURROGATE KEY
# ============================================================

window_sk = Window.orderBy("nk_fornecedor")


df_dim_fornecedor = (
    df_base_fornecedor
    .withColumn("sk_fornecedor", F.row_number().over(window_sk))
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .select(
        "sk_fornecedor",
        "nk_fornecedor",
        "cnpj_cpf_fornecedor",
        "cnpj_cpf_fornecedor_limpo",
        "nome_fornecedor",
        "tipo_pessoa",
        "updated_at",
        "pipeline_version"
    )
)

# ============================================================
# 5. ESCRITA DELTA SILVER
# Alterado a estrutura para merge, considerando a necessidade de manter o histórico dos deputados permitindo
# atualização e inserção de registros sem recriação completa das tabelas.
# ============================================================

executar_merge(
    df=df_dim_fornecedor,
    tabela_destino=TABELA_DESTINO,
    condicao_merge="""
        dest.nk_fornecedor = orig.nk_fornecedor
    """
)
