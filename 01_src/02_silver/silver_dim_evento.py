# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/99_utils/common_utils

# COMMAND ----------

# ============================================================
# SILVER - DIM_EVENTO
# Projeto Final - Engenharia de Dados
# Camada Silver | Constroe uma dimensão de eventos da Câmara dos Deputados.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_ORIGEM = "desafio_final_t2.bronze.bronze_eventos"
TABELA_TIPO_EVENTO = "desafio_final_t2.silver.dim_tipo_evento"
TABELA_DESTINO = "desafio_final_t2.silver.dim_evento"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA
# ============================================================

df_bronze_eventos = spark.table(TABELA_ORIGEM)
df_tipo_evento = spark.table(TABELA_TIPO_EVENTO)

# ============================================================
# 2.1 RECONSTRUÇÃO DE ESTRUTURAS JSON
# Identifiquei uma incompatibilidade de schema entre Bronze e Silver. 
# Como a Bronze passou a armazenar estruturas complexas em formato JSON para garantir estabilidade da 
# ingestão, realizei a reconstrução dessas estruturas na Silver utilizando from_json, preservando a semântica dos dados."
# ============================================================

schema_local_camara = """
struct<
    nome:string,
    predio:string,
    sala:string,
    andar:string
>
"""

df_bronze_eventos = (
    df_bronze_eventos
    .withColumn(
        "localCamara",
        F.from_json(
            F.col("localCamara"),
            schema_local_camara
        )
    )
)


# ============================================================
# 3. TRANSFORMAÇÃO BASE
# ============================================================

df_eventos_base = (
    df_bronze_eventos
    .select(
        F.col("id").cast("long").alias("nk_evento"),
        F.col("uri").cast("string").alias("uri_evento"),
        F.to_timestamp("dataHoraInicio", "yyyy-MM-dd'T'HH:mm").alias("data_hora_inicio"),
        F.to_timestamp("dataHoraFim", "yyyy-MM-dd'T'HH:mm").alias("data_hora_fim"),
        F.col("situacao").cast("string").alias("situacao_evento"),
        F.col("descricaoTipo").cast("string").alias("descricao_tipo_evento"),
        F.col("descricao").cast("string").alias("descricao_evento"),
        F.col("localExterno").cast("string").alias("local_externo"),
        F.col("localCamara.nome").cast("string").alias("local_camara_nome"),
        F.col("localCamara.predio").cast("string").alias("local_camara_predio"),
        F.col("localCamara.sala").cast("string").alias("local_camara_sala"),
        F.col("localCamara.andar").cast("string").alias("local_camara_andar"),
        F.col("urlRegistro").cast("string").alias("url_registro"),
        F.col("orgaos").alias("orgaos"),
        F.col("raw_payload").cast("string").alias("raw_payload"),
        F.col("ingested_at").cast("timestamp").alias("bronze_ingested_at")
    )
    .dropDuplicates(["nk_evento"])
)


# ============================================================
# 4. JOIN COM DIM_TIPO_EVENTO
# ============================================================

df_evento_tipo = (
    df_eventos_base.alias("e")
    .join(
        df_tipo_evento.alias("t"),
        F.upper(F.trim(F.col("e.descricao_tipo_evento"))) == F.upper(F.trim(F.col("t.nome_tipo_evento"))),
        "left"
    )
    .select(
        F.col("e.nk_evento"),
        F.col("e.uri_evento"),
        F.col("e.data_hora_inicio"),
        F.col("e.data_hora_fim"),
        F.col("e.situacao_evento"),
        F.col("e.descricao_tipo_evento"),
        F.col("e.descricao_evento"),
        F.col("e.local_externo"),
        F.col("e.local_camara_nome"),
        F.col("e.local_camara_predio"),
        F.col("e.local_camara_sala"),
        F.col("e.local_camara_andar"),
        F.col("e.url_registro"),
        F.col("e.orgaos"),
        F.col("t.sk_tipo_evento"),
        F.col("t.cod_tipo_evento"),
        F.col("e.raw_payload"),
        F.col("e.bronze_ingested_at")
    )
)


# ============================================================
# 5. CAMPOS DERIVADOS DE DATA
# ============================================================

df_evento_tipo = (
    df_evento_tipo
    .withColumn("data_evento", F.to_date("data_hora_inicio"))
    .withColumn("ano_evento", F.year("data_hora_inicio"))
    .withColumn("mes_evento", F.month("data_hora_inicio"))
    .withColumn("semana_evento", F.weekofyear("data_hora_inicio"))
)


# ============================================================
# 6. SURROGATE KEY
# ============================================================

window_sk = Window.orderBy("nk_evento")

df_dim_evento = (
    df_evento_tipo
    .withColumn("sk_evento", F.row_number().over(window_sk))
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .select(
        "sk_evento",
        "nk_evento",
        "uri_evento",
        "sk_tipo_evento",
        "cod_tipo_evento",
        "data_hora_inicio",
        "data_hora_fim",
        "data_evento",
        "ano_evento",
        "mes_evento",
        "semana_evento",
        "situacao_evento",
        "descricao_tipo_evento",
        "descricao_evento",
        "local_externo",
        "local_camara_nome",
        "local_camara_predio",
        "local_camara_sala",
        "local_camara_andar",
        "url_registro",
        "orgaos",
        "raw_payload",
        "bronze_ingested_at",
        "updated_at",
        "pipeline_version"
    )
)


# ============================================================
# 7. DEDUPLICAÇÃO E ESCRITA DELTA SILVER
# ============================================================

# Remover duplicatas mantendo o registro mais recente por nk_evento
df_dim_evento_dedupe = (
    df_dim_evento
    .dropDuplicates(["nk_evento"])
)

if spark.catalog.tableExists(TABELA_DESTINO):

    salvar_delta(
        df=df_dim_evento_dedupe,
        tabela=TABELA_DESTINO,
        usar_merge=True,
        chaves_merge=["nk_evento"]
    )


else:

    (
    df_dim_evento.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABELA_DESTINO)
    )

