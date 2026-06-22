# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/99_utils/common_utils

# COMMAND ----------

# ============================================================
# SILVER - DIM_DEPUTADO
# Projeto Final - Engenharia de Dados
# Camada Silver | Cria dimensão de deputados com atributos políticos e contato.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_ORIGEM = "desafio_final_t2.bronze.bronze_deputados"
TABELA_DESTINO = "desafio_final_t2.silver.dim_deputado"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA BRONZE
# ============================================================

df_bronze = spark.table(TABELA_ORIGEM)


# ============================================================
# 3. TRANSFORMAÇÃO SILVER
# Normaliza dados de deputados
# ============================================================

df_silver = (
    df_bronze
    .select(
        F.col("id").cast("long").alias("nk_deputado"),
        F.col("uri").cast("string").alias("uri_deputado"),
        F.col("nome").cast("string").alias("nome_deputado"),
        F.col("siglapartido").cast("string").alias("sigla_partido"),
        F.regexp_extract(F.col("uripartido"), r"/partidos/(\d+)", 1).cast("long").alias("id_partido"),
        F.col("siglauf").cast("string").alias("sigla_uf"),
        F.col("idlegislatura").cast("int").alias("id_legislatura"),
        F.col("urlfoto").cast("string").alias("url_foto"),
        F.col("email").cast("string").alias("email"),
        F.col("ingested_at").cast("timestamp").alias("bronze_ingested_at")
    )
    .dropDuplicates([
        "nk_deputado",
        "sigla_partido",
        "sigla_uf",
        "id_legislatura"
    ])
)


# ============================================================
# 4. SCD TYPE 2 SIMPLIFICADO
# Implementa (parcialmente) SCD Type 2 para acompanhar mudanças de partido, estado e legislatura dos deputados.
# ============================================================

window_sk = Window.orderBy("nk_deputado", "sigla_partido", "sigla_uf", "id_legislatura")

df_dim_deputado = (
    df_silver
    .withColumn("sk_deputado", F.row_number().over(window_sk))
    .withColumn("valid_from", F.current_date())
    .withColumn("valid_to", F.lit(None).cast("date"))
    .withColumn("is_current", F.lit(True))
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .select(
        "sk_deputado",
        "nk_deputado",
        "uri_deputado",
        "nome_deputado",
        "sigla_partido",
        "id_partido",
        "sigla_uf",
        "id_legislatura",
        "url_foto",
        "email",
        "valid_from",
        "valid_to",
        "is_current",
        "bronze_ingested_at",
        "updated_at",
        "pipeline_version"
    )
)


# ============================================================
# 5. ESCRITA DELTA SILVER
# Realiza merge incremental na dimensão de deputados, atualizando registros existentes e inserindo novos conforme necessário.
# ============================================================

salvar_delta(
    df=df_dim_deputado,
    tabela=TABELA_DESTINO,
    usar_merge=True,
    chaves_merge=["nk_deputado"]
)

