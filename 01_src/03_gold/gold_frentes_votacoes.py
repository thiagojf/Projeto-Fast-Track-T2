# ============================================================
# GOLD - GOLD_FRENTES_VOTACOES
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

TABELA_DESTINO = "desafio_final_t2.gold.gold_frentes_votacoes"

PIPELINE_VERSION = "1.0"


# ============================================================
# 2. LEITURA E TRANSFORMAÇÃO
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

),voto_majoritario AS (

    SELECT
        nk_frente,
        id_votacao,
        voto,
        COUNT(*) AS qtde_votos,
        -- Aqui contamos quantos votos de cada tipo ocorreram em cada votação.
        -- Em caso de empate é considerado o primeiro voto retornado pela ordenação da janela.
        DENSE_RANK() OVER (
            PARTITION BY nk_frente, id_votacao
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM base_votos
    GROUP BY
        nk_frente,
        id_votacao,
        voto

),alinhamento AS (
-- Agora calculamos quem acompanhou o voto majoritário.
    SELECT
        b.nk_frente,
        b.titulo_frente,
        b.id_votacao,

        COUNT(*) AS total_deputados,

        SUM(
            CASE
                WHEN b.voto = vm.voto
                THEN 1
                ELSE 0
            END
        ) AS deputados_alinhados

    FROM base_votos b

    INNER JOIN voto_majoritario vm
        ON b.nk_frente = vm.nk_frente
       AND b.id_votacao = vm.id_votacao
       AND vm.rn = 1 -- filtro de voto majoritário iguais

    GROUP BY
        b.nk_frente,
        b.titulo_frente,
        b.id_votacao

)

SELECT
    nk_frente,
    titulo_frente,
    id_votacao,
    deputados_alinhados,
    total_deputados,
    ROUND(
        deputados_alinhados * 100.0 / total_deputados,2
    ) AS percent_alinhamento,
    CASE
    WHEN percent_alinhamento >= 80 THEN 'Alto'
    WHEN percent_alinhamento >= 60 THEN 'Médio'
    ELSE 'Baixo'
END AS faixa_alinhamento
FROM alinhamento
""")


# ============================================================
# 3. AUDITORIA
# ============================================================

df_gold_frentes_votacoes = (
    df_gold_frentes_votacoes
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
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
