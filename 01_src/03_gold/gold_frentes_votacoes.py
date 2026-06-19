# Databricks notebook source
# ============================================================
# GOLD - FRENTES VOTAÇÕES (REFATORADO)
# # Projeto Final - Engenharia de Dados
# Camada Gold | Disponibiliza Indicador de alinhamento de votação por frente parlamentar
# ============================================================

from pyspark.sql import functions as F

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.gold_frentes_votacoes"
PIPELINE_VERSION = "1.1"

# ============================================================
# 2. CÁLCULO BASE DE VOTOS
# ============================================================

df_gold_frentes_votacoes = spark.sql("""
WITH base_votos AS (

    SELECT
        frente.nk_frente,
        frente.titulo_frente,
        voto.id_votacao,
        voto.nk_deputado,
        voto.voto
    FROM desafio_final_t2.silver.bridge_frente_deputado dep_frente
    INNER JOIN desafio_final_t2.silver.dim_frente frente
        ON dep_frente.nk_frente = frente.nk_frente
    INNER JOIN desafio_final_t2.silver.dim_votos_votacoes voto
        ON dep_frente.nk_deputado = voto.nk_deputado

),
-- 2.1 AGREGAÇÃO DE VOTOS POR OPÇÃO
voto_aggregado AS (

    SELECT
        nk_frente,
        id_votacao,
        voto,
        COUNT(*) AS qtde_votos
    FROM base_votos
    GROUP BY nk_frente, id_votacao, voto

),
-- 2.2 IDENTIFICA VOTO MAJORITÁRIO (DESEMPATE DETERMINÍSTICO)
voto_majoritario AS (

    SELECT *
    FROM (
        SELECT
            nk_frente,
            id_votacao,
            voto,
            qtde_votos,
            ROW_NUMBER() OVER (
                PARTITION BY nk_frente, id_votacao
                ORDER BY qtde_votos DESC, voto ASC
            ) AS rn
        FROM voto_aggregado
    ) t
    WHERE rn = 1

),
-- 2.3 CÁLCULO DE ALINHAMENTO
alinhamento_base AS (

    SELECT
        b.nk_frente,
        b.titulo_frente,
        b.id_votacao,
        COUNT(*) AS total_deputados,
        SUM(
            CASE
                WHEN b.voto = vm.voto THEN 1
                ELSE 0
            END
        ) AS deputados_alinhados
    FROM base_votos b
    INNER JOIN voto_majoritario vm
        ON b.nk_frente = vm.nk_frente
       AND b.id_votacao = vm.id_votacao
    GROUP BY
        b.nk_frente,
        b.titulo_frente,
        b.id_votacao

),
-- 2.4 INDICADOR FINAL
final AS (

    SELECT
        nk_frente,
        titulo_frente,
        id_votacao,
        deputados_alinhados,
        total_deputados,

        ROUND(
            deputados_alinhados * 100.0 / total_deputados,
            2
        ) AS percent_alinhamento

    FROM alinhamento_base

)

SELECT
    *,
    CASE
        WHEN percent_alinhamento >= 80 THEN 'Alto'
        WHEN percent_alinhamento >= 60 THEN 'Médio'
        ELSE 'Baixo'
    END AS faixa_alinhamento

FROM final
""")

# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_frentes_votacoes = (
    df_gold_frentes_votacoes
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .withColumn("source_layer", F.lit("silver"))
    .withColumn("source_system", F.lit("camara_dos_deputados"))
)

# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    df_gold_frentes_votacoes.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)