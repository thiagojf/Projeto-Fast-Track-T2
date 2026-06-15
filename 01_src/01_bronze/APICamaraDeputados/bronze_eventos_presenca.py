# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

# ============================================================
# BRONZE_EVENTOS_PRESENCA
# Camada Bronze | Presença de deputados por evento
# Endpoint: /eventos/{id}/deputados
# ============================================================

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

import json
import time
import uuid

from pyspark.sql import functions as F

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

TABELA_ORIGEM_EVENTOS = "desafio_final_t2.bronze.bronze_eventos"
TABELA_DESTINO = "desafio_final_t2.bronze.bronze_eventos_presenca"

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())


# ============================================================
# 2. OBTÉM IDS DOS EVENTOS
# ============================================================

ids_eventos = [

    row["id"]

    for row in (
        spark.table(TABELA_ORIGEM_EVENTOS)
        .select("id")
        .distinct()
        .collect()
    )
]

total_eventos = len(ids_eventos)

log_info(
    f"Total de eventos para processar: {total_eventos}"
)


# ============================================================
# 3. CONTROLE DE ERROS
# ============================================================

lista_erros = []


# ============================================================
# 4. PROCESSAMENTO DOS EVENTOS
# ============================================================

for contador, id_evento in enumerate(ids_eventos, start=1):

    try:

        endpoint_atual = f"/eventos/{id_evento}/deputados"

        url_atual = (
            f"{BASE_URL}{endpoint_atual}"
        )

        log_info(
            f"Processando evento "
            f"{contador}/{total_eventos} "
            f"- ID {id_evento}"
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
        # EVENTO SEM PRESENÇA
        # ============================================

        if not dados:

            log_warning(
                f"Evento {id_evento} sem presença."
            )

            continue

        # ============================================
        # PREPARAÇÃO DOS DADOS
        # ============================================

        lista_presenca = []

        for deputado in dados:

            deputado["id_evento"] = id_evento

            deputado[
                "source_endpoint_detail"
            ] = endpoint_atual

            deputado["raw_payload"] = json.dumps(
                deputado,
                ensure_ascii=False
            )

            lista_presenca.append(
                deputado
            )

        # ============================================
        # JSON -> DATAFRAME
        # ============================================

        json_strings = [

            json.dumps(
                registro,
                ensure_ascii=False
            )

            for registro in lista_presenca

        ]

        spark_df_raw = spark.createDataFrame(
            [(item,) for item in json_strings],
            ["json_string"]
        )

        schema_presenca = """
        struct<
            id:bigint,
            uri:string,
            nome:string,
            siglaPartido:string,
            uriPartido:string,
            siglaUf:string,
            idLegislatura:bigint,
            urlFoto:string,
            email:string,
            id_evento:bigint,
            source_endpoint_detail:string,
            raw_payload:string
        >
        """

        spark_df = (

            spark_df_raw

            .select(
                F.from_json(
                    F.col("json_string"),
                    schema_presenca
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
        # DEFINIÇÃO MODO DE ESCRITA
        # ============================================

        if not spark.catalog.tableExists(
            TABELA_DESTINO
        ):

            modo_escrita = "overwrite"

        else:

            modo_escrita = "append"

        # ============================================
        # ESCRITA DELTA
        # ============================================

        salvar_delta(
            df=spark_df,
            tabela=TABELA_DESTINO,
            modo=modo_escrita,
            particionar=True,
            colunas_particao=[
                "ano_ingestao",
                "mes_ingestao"
            ]
        )

        log_info(
            f"Evento {id_evento} gravado com sucesso."
        )

        time.sleep(0.2)

    except Exception as e:

        log_error(
            f"Erro no evento {id_evento}: {str(e)}"
        )

        lista_erros.append({

            "id_evento": id_evento,

            "endpoint": endpoint_atual,

            "erro": str(e)

        })

        continue


# ============================================================
# 5. RESUMO EXECUÇÃO
# ============================================================

log_info(
    f"Processamento concluído. "
    f"Eventos com erro: {len(lista_erros)}"
)

if lista_erros:

    log_warning(
        "Eventos com falha durante a execução:"
    )

    for erro in lista_erros:

        print(erro)

log_info(
    "Ingestão concluída com sucesso."
)