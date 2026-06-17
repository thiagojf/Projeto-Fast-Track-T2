# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

# ============================================================
#  SILVER - DIM VOTACÃO
# Implementa pipeline Silver para construção da tabela dim_votos_votacoes
# Realiza leitura das tabelas bronze_votacoes_votos e bronze_votacoes
# Extrai e padroniza informações de votação, deputado e voto
# Enriquece os dados com resultado, descrição da votação e órgão responsável
# Obtém id_evento a partir da URI do evento
# Gera chave substituta (nk_voto) baseada em id_votacao e deputado
# Adiciona metadados de rastreabilidade (updated_at e pipeline_version)
# Implementa carga incremental utilizando MERGE em tabela Delta
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import explode

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_ORIGEM = "desafio_final_t2.bronze.bronze_votacoes_votos"
TABELA_BRONZE_VOTACOES = "desafio_final_t2.bronze.bronze_votacoes"
TABELA_DESTINO = "desafio_final_t2.silver.dim_votos_votacoes"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA BRONZE
# ============================================================

df_bronze_votacoes_votos = spark.table(TABELA_ORIGEM)
df_bronze_votacoes = spark.table(TABELA_BRONZE_VOTACOES)

# ============================================================
# 3. TRANSFORMAÇÃO bronze_votacoes_votos
# ============================================================

df_silver_votacoes_votos_explodido = (
    df_bronze_votacoes_votos
    .select(
        F.col("id_votacao").alias("id_votacao"),
        F.col("dataRegistroVoto").cast("string").alias("data_votacao"),
        F.col("deputado_").alias("deputado"),
        F.col("tipoVoto").cast("string").alias("voto"),
        F.col("dataRegistroVoto").cast("string").alias("data_voto"),
        F.col("deputado_.id").cast("long").alias("nk_deputado")
    )
)

# ============================================================
# 4. JOIN COM bronze_votacoes
# ============================================================
df_result_join = (
    df_silver_votacoes_votos_explodido.alias("df_bvv")
    .join(
        df_bronze_votacoes.alias("df_bv"),
        F.col("df_bvv.id_votacao") == F.col("df_bv.id"),
        "left"
    )
    .select(
            F.col("df_bvv.id_votacao"),
            F.regexp_extract(F.col("uriEvento"),r"(\d+)$",1).cast("long").alias("id_evento"),
            F.col("df_bvv.nk_deputado"),
            F.col("df_bvv.voto"),
            F.col("df_bvv.data_voto"),
            F.col("df_bv.aprovacao").alias("resultado_votacao"),
            F.col("df_bv.descricao").alias("descricao_votacao"),
            F.col("df_bv.siglaOrgao").alias("sigla_orgao"),
      
    )
)

# ============================================================
# 5. SURROGATE KEY
# ============================================================

df_result_join = (
    df_result_join
    .withColumn("nk_voto",F.sha2(F.concat_ws("|",F.col("id_votacao"),F.col("nk_deputado")),256))
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .select(
            "nk_voto",
            "id_votacao",
            "id_evento",
            "nk_deputado",
            "voto",
            "data_voto",
            "resultado_votacao",
            "descricao_votacao",
            "sigla_orgao",
            "updated_at",
            "pipeline_version"
    )
)

# ============================================================
# 6. ESCRITA DELTA SILVER
# ============================================================

executar_merge(
    df=df_result_join,
    tabela_destino=TABELA_DESTINO,
    condicao_merge="""
        dest.nk_voto = orig.nk_voto
    """
)
