# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

# ============================================================
# BRONZE_TIPO_EVENTO
# Camada Bronze | Referência de tipos de eventos
# Endpoint: /referencias/eventos/codTipoEvento
# ============================================================

import requests
import time
import uuid
import json
from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
ENDPOINT = "/referencias/eventos/codTipoEvento"
URL = f"{BASE_URL}{ENDPOINT}"

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())

TABELA_DESTINO = "desafio_final_t2.bronze.bronze_tipo_evento"


# ============================================================
# 3. INGESTÃO
# ============================================================
pagina = 1

try:
    log_info(
        f"Coletando página {pagina}"
    )

    # ============================================
    # REQUEST API
    # ============================================

    response_json = get_api_data(
        url=URL,
        max_retries=MAX_RETRIES,
        retry_delay=RETRY_DELAY,
        timeout=TIMEOUT
    )

    dados = response_json.get("dados", [])


    # ============================================
    # CONDIÇÃO DE PARADA
    # ============================================

    if not dados:

        log_info(
            "Nenhum registro encontrado. Finalizando ingestão."
        )

    # ============================================
    # RAW PAYLOAD
    # ============================================

    lista_tipo_evento = []

    for tipo_evento in dados:

        tipo_evento["source_endpoint_detail"] = ENDPOINT
        tipo_evento["raw_payload"] = json.dumps(
            tipo_evento,
            ensure_ascii=False
        )

        lista_tipo_evento.append(tipo_evento)

    print(f"[INFO] Total de tipos de evento coletados: {len(lista_tipo_evento)}")


    # ============================================================
    # CRIAÇÃO SPARK DATAFRAME
    # Compatível com Databricks Serverless
    # ============================================================

    json_strings = [
        json.dumps(registro, ensure_ascii=False)
        for registro in lista_tipo_evento
    ]

    spark_df_raw = spark.createDataFrame(
        [(item,) for item in json_strings],
        ["json_string"]
    )

    schema_tipo_evento = """
    struct<
        cod:string,
        sigla:string,
        nome:string,
        descricao:string,
        source_endpoint_detail:string,
        raw_payload:string
    >
    """

    spark_df = (
        spark_df_raw
        .select(
            F.from_json(
                F.col("json_string"),
                schema_tipo_evento
            ).alias("dados")
        )
        .select("dados.*")
    )



    # ============================================
    # AUDITORIA
    # ============================================

    spark_df = adicionar_auditoria(
        df=spark_df,
        endpoint=ENDPOINT,
        batch_id=BATCH_ID,
        pipeline_version=PIPELINE_VERSION
    )


    # ============================================
    # ESCRITA DELTA
    # Como é uma tabela de domínio quero sempre quero a versão mais recente da referência.
    # ============================================
    salvar_delta(
        df=spark_df,
        tabela=TABELA_DESTINO,
        modo="overwrite",
        overwrite_schema=True,
        particionar=False
    )

    log_info(
        f"{len(lista_tipo_evento)} registros gravados com sucesso."
    )

except Exception as e:

    log_error(
        f"Erro na página {pagina}: {str(e)}"
    )

    raise
        

# ============================================================
# 4. FINALIZAÇÃO
# ============================================================

log_info(
    "Ingestão concluída com sucesso."
)