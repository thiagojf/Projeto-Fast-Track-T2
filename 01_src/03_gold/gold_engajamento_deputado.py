# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

# DBTITLE 1,Análise de Engajamento dos Deputados
# ============================================================
# GOLD - Score de engajamento composto: presença × votações
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.gold_engajamento_deputado"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
# ============================================================
df_gold_engajamento_deputado = spark.sql("""
WITH deputados_presenca AS (

    SELECT
        dep.nk_deputado,
        dep.nome_deputado,
        dep.sigla_partido,
        dep.sigla_uf,
        COUNT(DISTINCT pres.id_evento) AS qtd_eventos_presente

    FROM desafio_final_t2.silver.dim_deputado dep
    INNER JOIN desafio_final_t2.bronze.bronze_eventos_presenca pres  ON dep.nk_deputado = pres.id

    GROUP BY
        dep.nk_deputado,
        dep.nome_deputado,
        dep.sigla_partido,
        dep.sigla_uf
),votacoes_participadas AS (

 select  
    dep_p.nk_deputado,
    dep_p.nome_deputado,	
    dep_p.sigla_partido,	
    dep_p.sigla_uf,
    dep_p.qtd_eventos_presente,
    COUNT(DISTINCT vot.id_votacao) as qtd_votacoes_participadas,
    SUM(
        CASE
            WHEN UPPER(vot.voto) = 'SIM'
            THEN 1
            ELSE 0
        END
    ) AS qtd_votos_sim,
    SUM(
        CASE
            WHEN UPPER(vot.voto) = 'NÃO'
            THEN 1
            ELSE 0
        END
    ) AS qtd_votos_nao,
    SUM(
        CASE
            WHEN UPPER(vot.voto) = 'ABSTENÇÃO'
            THEN 1
            ELSE 0
        END
    ) AS qtd_abstencoes,

    SUM(
        CASE
            WHEN UPPER(vot.voto) = 'OBSTRUÇÃO'
            THEN 1
            ELSE 0
        END
    ) AS qtd_obstrucoes,

    SUM(
        CASE
            WHEN UPPER(vot.voto) = 'ART. 17'
            THEN 1
            ELSE 0
        END
    ) AS qtd_art17,

    SUM(
        CASE
            WHEN UPPER(vot.voto) = 'LIBERADO'
            THEN 1
            ELSE 0
        END
    ) AS qtd_liberado
    from deputados_presenca dep_p
    inner join desafio_final_t2.silver.dim_votos_votacoes vot on dep_p.nk_deputado = vot.nk_deputado
    group by
    dep_p.nk_deputado,
    dep_p.nome_deputado,	
    dep_p.sigla_partido,	
    dep_p.sigla_uf,
    dep_p.qtd_eventos_presente

),score_engajamento AS (
select  
    nk_deputado,
    nome_deputado,	
    sigla_partido,	
    sigla_uf,
    qtd_eventos_presente,
    qtd_votos_sim,
    qtd_votos_nao,
    qtd_abstencoes,
    qtd_obstrucoes,
    qtd_art17,
    qtd_liberado,
    qtd_votacoes_participadas,
    (
        (
            qtd_eventos_presente
            / MAX(qtd_eventos_presente) OVER()
            * 100
        ) * 0.4
    )
    +
    (
        (
            qtd_votacoes_participadas
            / MAX(qtd_votacoes_participadas) OVER()
            * 100
        ) * 0.6
    )
    AS score_engajamento
from votacoes_participadas
)
SELECT 
*,
    DENSE_RANK() OVER(
    ORDER BY score_engajamento DESC
) as ranking_engajamento,
PERCENT_RANK() OVER(
    ORDER BY score_engajamento
) percentil_engajamento
FROM score_engajamento
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_engajamento_deputado = (
    df_gold_engajamento_deputado
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ============================================================
# 4. ESCRITA DELTA GOLD
# ============================================================

(
    df_gold_engajamento_deputado.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from desafio_final_t2.gold.gold_engajamento_deputado