# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

# ============================================================
# BRONZE_VOTACOES_VOTOS
# Camada Bronze | Votos dos Deputados em Votações
# Realiza a ingestão dos votos individuais dos deputados em cada votação registrada na Câmara dos Deputados.
# Endpoint: /votacoes/{id}/votos
# ============================================================

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

import json
import time
import uuid

from pyspark.sql import functions as F

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"


MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())

## Tabelas de origem e destino
TABELA_ORIGEM = "desafio_final_t2.bronze.bronze_votacoes"
TABELA_DESTINO = "desafio_final_t2.bronze.bronze_votacoes_votos"

# ============================================================
# 2. OBTÉM IDS DAS VOTAÇÕES PARA PROCESSAR
# Recupera os IDs das votações previamente carregadas para enriquecimento dos dados com os votos individuais dos deputados.
# ============================================================

#id_votacao = ["2085970-127"]

id_votacao = [

    row["id"]

    for row in (
        spark.table(TABELA_ORIGEM)
        .select("id")
        .distinct()
        .toLocalIterator()
    )
]

total_votacoes = len(id_votacao)

log_info(
    f"Total de Votações para processar: {total_votacoes}"
)


# ============================================================
# 3. CONTROLE DE ERROS
# ============================================================

lista_erros = []


# ============================================================
# 4. PROCESSAMENTO DOS VOTOS
# Consulta o endpoint de votos para cada votação retornando o posicionamento individual dos deputados
# ============================================================

for contador, id_votacao in enumerate(id_votacao, start=1):

    try:

        endpoint_atual = f"/votacoes/{id_votacao}/votos"

        url_atual = (
            f"{BASE_URL}{endpoint_atual}"
        )

        log_info(
            f"Processando Votação "
            f"{contador}/{total_votacoes} "
            f"- ID {id_votacao}"
        )

        # ============================================
        # REQUEST API
        # ============================================

        response_json = get_api_data(
            url=url_atual,
            max_retries=MAX_RETRIES,
            retry_delay=RETRY_DELAY,
            timeout=TIMEOUT
        )

        dados = response_json.get(
            "dados",
            []
        )

        # ============================================
        # VOTACAO SEM VOTO
        # ============================================

        if not dados:

            log_warning(
                f"Votacao {id_votacao} sem Voto."
            )

            continue

        # ============================================
        # PREPARAÇÃO DOS DADOS
        # ============================================

        lista_votos  = []

        for votacao in dados:

            votacao["id_votacao"] = id_votacao

            votacao[
                "source_endpoint_detail"
            ] = endpoint_atual

            votacao["raw_payload"] = json.dumps(
                votacao,
                ensure_ascii=False
            )

            lista_votos.append(
                votacao
            )

        # ============================================
        # JSON -> DATAFRAME
        # ============================================

        json_strings = [

            json.dumps(
                registro,
                ensure_ascii=False
            )

            for registro in lista_votos 

        ]

        spark_df_raw = spark.createDataFrame(
            [(item,) for item in json_strings],
            ["json_string"]
        )

        schema_votos = """
        struct<
            dataRegistroVoto:string,
            tipoVoto:string,
            deputado_:struct<
                id:bigint,
                uri:string,
                nome:string,
                siglaPartido:string,
                uriPartido:string,
                siglaUf:string,
                idLegislatura:bigint,
                urlFoto:string,
                email:string
            >,
            id_votacao:string,
            source_endpoint_detail:string,
            raw_payload:string
        >
        """

        spark_df = (

            spark_df_raw

            .select(
                F.from_json(
                    F.col("json_string"),
                    schema_votos
                ).alias("dados")
            )

            .select("dados.*")

        )

        # ============================================
        # AUDITORIA
        # ============================================

        spark_df = adicionar_auditoria(
            df=spark_df,
            endpoint=endpoint_atual,
            batch_id=BATCH_ID,
            pipeline_version=PIPELINE_VERSION
        )

        # ============================================
        # ESCRITA DELTA
        # Tabelas históricas  modo= "append" porque representam fatos que crescem ao longo do tempo.
        # ============================================

        salvar_delta(
            df=spark_df,
            tabela=TABELA_DESTINO,
            modo= "append",
            particionar=True,
            colunas_particao=[
                "ano_ingestao",
                "mes_ingestao"
            ]
        )

        log_info(
            f"Votacao {id_votacao} gravada com sucesso."
        )

        time.sleep(0.2)

    except Exception as e:

        log_error(
            f"Erro no Votacao {id_votacao}: {str(e)}"
        )

        lista_erros.append({

            "id_votacao": id_votacao,

            "endpoint": endpoint_atual,

            "erro": str(e)

        })

        continue

