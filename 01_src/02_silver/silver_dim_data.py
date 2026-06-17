# Databricks notebook source
# ============================================================
# SILVER - DIM_DATA
# Tabela calendário reutilizável
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.silver.dim_data"

DATA_INICIO = "2023-01-01"
DATA_FIM = "2027-12-31"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. GERAÇÃO DO CALENDÁRIO
# ============================================================

df_dim_data = (
    spark.sql(f"""
        SELECT explode(sequence(
            to_date('{DATA_INICIO}'),
            to_date('{DATA_FIM}'),
            interval 1 day
        )) AS data
    """)
)


# ============================================================
# 3. ATRIBUTOS CALENDÁRIOS
# ============================================================

df_dim_data = (
    df_dim_data
    .withColumn("sk_data", F.date_format(F.col("data"), "yyyyMMdd").cast("int"))
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("nome_mes", F.date_format("data", "MMMM"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
    .withColumn("semana_ano", F.weekofyear("data"))
    .withColumn("dia_mes", F.dayofmonth("data"))
    .withColumn("dia_semana", F.dayofweek("data"))
    .withColumn("nome_dia_semana", F.date_format("data", "EEEE"))
    .withColumn(
        "is_fim_semana",
        F.when(F.col("dia_semana").isin(1, 7), F.lit(True)).otherwise(F.lit(False))
    )
)


# ============================================================
# 4. FLAGS DE PERÍODO ELEITORAL
# Regra simplificada:
# Ano eleitoral geral no Brasil: 2026
# ============================================================

df_dim_data = (
    df_dim_data
    .withColumn(
        "periodo_eleitoral",
        F.when(
            (F.col("data") >= F.lit("2026-08-01").cast("date")) &
            (F.col("data") <= F.lit("2026-10-31").cast("date")),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .withColumn(
        "descricao_periodo_eleitoral",
        F.when(F.col("periodo_eleitoral") == True, F.lit("Período eleitoral 2026"))
         .otherwise(F.lit("Fora do período eleitoral"))
    )
)


# ============================================================
# 5. AUDITORIA
# ============================================================

df_dim_data = (
    df_dim_data
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .select(
        "sk_data",
        "data",
        "ano",
        "mes",
        "nome_mes",
        "trimestre",
        "ano_mes",
        "semana_ano",
        "dia_mes",
        "dia_semana",
        "nome_dia_semana",
        "is_fim_semana",
        "periodo_eleitoral",
        "descricao_periodo_eleitoral",
        "updated_at",
        "pipeline_version"
    )
)

# ============================================================
# 6. ESCRITA DELTA SILVER
# ============================================================

(
    df_dim_data.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)

