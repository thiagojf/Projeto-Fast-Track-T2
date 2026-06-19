# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

# ============================================================
# BRONZE_ORGAOS
# Camada Bronze
# Realiza a ingestão dos dados de órgãos da Câmara dos Deputados através da API de Dados Abertos
# Endpoint: /orgaos
# ============================================================

import time
import uuid
import json

from pyspark.sql import functions as F

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================
# URL base da API de Dados Abertos da Câmara dos Deputados
BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
ENDPOINT = "/orgaos"
URL = f"{BASE_URL}{ENDPOINT}"

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30
ITENS_POR_PAGINA = 100

# Versão lógica do pipeline
PIPELINE_VERSION = "1.0"

# Identificador único da execução para rastreabilidade
BATCH_ID = str(uuid.uuid4())

## Tabela de Destino
TABELA_DESTINO = "desafio_final_t2.bronze.bronze_orgaos"

# ============================================================
# 2. INGESTÃO PAGINADA
# ============================================================

pagina = 1
lista_orgaos = []

try:
    # Realiza paginação até que a API retorne uma página vazia
    while True:

        log_info(
            f"Coletando página {pagina}"
        )

        # ============================================
        # REQUEST API
        # ============================================

        response_json = get_api_data(
            url=URL,
            params={
                "sigla": "CPI",
                "pagina": pagina,
                "itens": ITENS_POR_PAGINA
            },
            max_retries=MAX_RETRIES,
            retry_delay=RETRY_DELAY,
            timeout=TIMEOUT
        )

        dados = response_json.get(
            "dados",
            []
        )

        # ============================================
        # CONDIÇÃO DE PARADA
        # ============================================

        if not dados:

            log_info(
                f"Fim da paginação. Página {pagina} sem registros."
            )

            break

        # ============================================
        # ACUMULA DADOS
        # ============================================

        for orgao in dados:
            # Mantém a origem do dado para rastreabilidade futura
            orgao["source_endpoint_detail"] = ENDPOINT
            
            # Armazena o payload original para auditoria, reprocessamento e troubleshooting 
            orgao["raw_payload"] = json.dumps(
                orgao,
                ensure_ascii=False
            )

            lista_orgaos.append(
                orgao
            )

        log_info(
            f"Página {pagina} processada. "
            f"Total acumulado: {len(lista_orgaos)}"
        )

        pagina += 1

        time.sleep(0.2)

    # ============================================
    # VALIDAÇÃO
    # ============================================

    if not lista_orgaos:

        raise Exception(
            "Nenhum órgão retornado pela API."
        )

    log_info(
        f"Total de órgãos coletados: {len(lista_orgaos)}"
    )

    # ============================================================
    # JSON -> DATAFRAME
    # ============================================================

    json_strings = [

        json.dumps(
            registro,
            ensure_ascii=False
        )

        for registro in lista_orgaos

    ]

    spark_df_raw = spark.createDataFrame(
        [(item,) for item in json_strings],
        ["json_string"]
    )

    schema_orgao = """
    STRUCT<
        id:string,
        sigla:string,
        nome:string,
        apelido:string,
        nomePublicacao:string,
        nomeResumido:string,
        tipoOrgao:string,
        codTipoOrgao:string,
        uri:string,
        source_endpoint_detail:string,
        raw_payload:string
    >
    """

    spark_df = (
        spark_df_raw
        .select(
            F.from_json(
                F.col("json_string"),
                schema_orgao
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
    # Como é uma tabela de domínio,
    # sempre mantemos a versão mais recente.
    # ============================================

    salvar_delta(
        df=spark_df,
        tabela=TABELA_DESTINO,
        modo="overwrite",
        overwrite_schema=True,
        particionar=False
    )

    log_info(
        f"{len(lista_orgaos)} registros gravados com sucesso."
    )


except Exception as e:

    log_error(
        f"Erro na ingestão: {str(e)}"
    )
    raise

# ============================================================
# 3. FINALIZAÇÃO
# ============================================================

log_info(
    "Ingestão concluída com sucesso."
)
