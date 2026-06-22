# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/99_utils/common_utils

# COMMAND ----------

# ============================================================
#  SILVER - DIM VOTACÃO
# Consolidar os votos individuais dos deputados em votações da Câmara dos Deputados.
# ============================================================

from pyspark.sql import functions as F

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_ORIGEM = "desafio_final_t2.bronze.bronze_votacoes_votos"
TABELA_BRONZE_VOTACOES = "desafio_final_t2.bronze.bronze_votacoes"
TABELA_DESTINO = "desafio_final_t2.silver.dim_votos_votacoes"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA BRONZE
# Carrega os votos individuais e os metadados das votações para construção da tabela fato
# ============================================================

df_bronze_votacoes_votos = spark.table(TABELA_ORIGEM)
df_bronze_votacoes = spark.table(TABELA_BRONZE_VOTACOES)

# ============================================================
# 3. TRANSFORMAÇÃO bronze_votacoes_votos
# Padroniza os atributos relevantes para análise e extrai a chave natural do deputado
# ============================================================

df_votos_normalizado = (
    df_bronze_votacoes_votos
    .select(
        F.col("id_votacao").alias("id_votacao"),
        F.col("dataRegistroVoto").cast("string").alias("data_votacao"),
        F.col("deputado_").alias("deputado"),
        F.col("tipoVoto").cast("string").alias("voto"),
        F.to_timestamp("dataRegistroVoto").alias("data_voto"),
        F.col("deputado_.id").cast("long").alias("nk_deputado")
    )
)

# ============================================================
# 4. JOIN COM bronze_votacoes
# Enriquecimento dos votos com informações da votação e vínculo com o evento correspondente
# ============================================================
df_result_join = (
    df_votos_normalizado.alias("df_bvv")
    .join(
        df_bronze_votacoes.alias("df_bv"),
        F.col("df_bvv.id_votacao") == F.col("df_bv.id"),
        "left"
    )
    .select(
            F.col("df_bvv.id_votacao"),
            # Extrai o identificador do evento a partir da URI retornada pela API
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
    .withColumn("ano_voto",F.year("data_voto"))
    .withColumn("mes_voto",F.month("data_voto"))
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
            "pipeline_version",
            "ano_voto",
            "mes_voto"
    )
)

# ============================================================
# 6. ESCRITA DELTA SILVER
# Atualiza registros existentes quando a chave já existe e insere novos registros quando a chave não é encontrada.
# Chave de negócio: nk_voto = hash(id_votacao + nk_deputado)
# ============================================================

salvar_delta(
    df=df_result_join,
    tabela=TABELA_DESTINO,
    usar_merge=True,
    chaves_merge=["nk_voto"]
)
