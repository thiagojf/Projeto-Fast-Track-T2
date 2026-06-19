# Databricks notebook source
# ============================================================
# SILVER - DIM CPI
# Camada Silver | Dimensão de Comissões Parlamentares de Inquérito (CPI)
#Consolidar informações dos órgãos classificados como CPI para composição da dimensão analítica de CPIs.
# ============================================================

from pyspark.sql import functions as F

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_BRONZE_ORGAOS = "desafio_final_t2.bronze.bronze_orgaos"
TABELA_BRONZE_ORGAOS_DETALHES = "desafio_final_t2.bronze.bronze_orgaos_detalhe"

TABELA_DESTINO = "desafio_final_t2.silver.silver_dim_cpi"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA DAS TABELAS
# Carrega os dados brutos e enriquecidos dos órgãos utilizados para construção da dimensão CPI
# ============================================================

df_orgaos= spark.table(TABELA_BRONZE_ORGAOS)
df_orgaos_detalhe = spark.table(TABELA_BRONZE_ORGAOS_DETALHES)

# ============================================================
# 3. NORMALIZAÇÃO DA BRONZE
# Seleciona apenas atributos relevantes para a dimensão e padroniza tipos de dados
# ============================================================

df_orgaos = (
    df_orgaos
    .select(
        F.col("id").cast("long").alias("nk_cpi"),
        F.col("sigla").cast("string").alias("sigla"),
        F.col("nome").cast("string").alias("nome_cpi"),
        F.col("codTipoOrgao").cast("int").alias("cod_tipo_orgao"),
        F.col("tipoOrgao").cast("string").alias("tipo_orgao"),
    )
    .dropDuplicates(["nk_cpi"])
    .filter(F.col("codTipoOrgao") == 4) # Mantém apenas órgãos classificados como CPI
)

# ============================================================
# 4. JOIN COM DIMENSÕES 
# ============================================================

df_dim_cpi = (
    df_orgaos.alias("o")

    .join(
        df_orgaos_detalhe.alias("od"),
        F.col("o.nk_cpi") == F.col("od.id_orgao"),
        "left"
    )

    .select(
         F.col("o.nk_cpi"),
         F.col("o.nome_cpi"),
         F.col("o.sigla"),
         F.col("o.tipo_orgao"),
         F.to_date("od.datainicio").alias("data_inicio"),
         F.to_date("od.datainstalacao").alias("data_instalacao"),
         F.to_date("od.datafim").alias("data_fim"),
         F.to_date("od.datafimoriginal").alias("data_fim_original"),
         F.col("od.urlWebsite").alias("url_website"),
         F.col("od.uri").alias("uri_orgao")
    )

    .dropDuplicates(["nk_cpi"])
)

# ============================================================
# 5. AUDITORIA
# ============================================================

df_dim_cpi = (
    df_dim_cpi
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .withColumn(
        "status_cpi",
        F.when(
            F.col("data_fim").isNull(),
            "Em andamento"
        )
        .when(
            F.col("data_fim") >= F.current_date(),
            "Em andamento"
        )
        .otherwise("Encerrada")
    )
)

# ============================================================
# 6. ESCRITA DELTA SILVER
# ============================================================

(
    df_dim_cpi.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)
