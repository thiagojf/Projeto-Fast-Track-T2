# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

# ============================================================
# BRONZE_FRENTES
# Projeto Final - Engenharia de Dados
# Camada Bronze | Ingerir frentes parlamentares da API da Câmara dos Deputados
# preservando dados brutos para modelagem posterior.
# ============================================================

# ============================================================
# 1. IMPORTS
# ============================================================

import time
import uuid
import json


# ============================================================
# 2. CONFIGURAÇÕES GERAIS
# ============================================================

# ----------------------------
# API
# ----------------------------

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
ENDPOINT = "/frentes"
URL = f"{BASE_URL}{ENDPOINT}"

# ----------------------------
# Retry / Timeout
# ----------------------------

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

# ----------------------------
# Paginação
# ----------------------------

ITENS_POR_PAGINA = 100

# ----------------------------
# Pipeline
# ----------------------------

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())

# ----------------------------
# Delta Destination
# ----------------------------

TABELA_DESTINO = "desafio_final_t2.bronze.bronze_frentes"

# ============================================================
# 3. SCHEMA BRONZE (evita inferência)
# ============================================================

SCHEMA_FRENTES = StructType([
    StructField("id", LongType(), True),
    StructField("uri", StringType(), True),
    StructField("titulo", StringType(), True),
    StructField("idLegislatura", LongType(), True),
    StructField("dataCriacao", StringType(), True),
    StructField("dataExtincao", StringType(), True),
    StructField("raw_payload", StringType(), True)
])

# ============================================================
# 3. INGESTÃO COM PAGINAÇÃO
# ============================================================

pagina = 1

while True:

    try:

        log_info(
            f"Coletando página {pagina}"
        )

        params = {
            "idLegislatura": 57,
            "pagina": pagina,
            "itens": ITENS_POR_PAGINA
        }

        # ============================================
        # REQUEST API
        # ============================================

        response_json = get_api_data(
            url=URL,
            params=params,
            max_retries=MAX_RETRIES,
            retry_delay=RETRY_DELAY,
            timeout=TIMEOUT
        )

        # ============================================
        # DADOS
        # ============================================

        dados = response_json.get(
            "dados",
            []
        )

        # ============================================
        # CONDIÇÃO DE PARADA
        # ============================================

        if not dados:

            log_info(
                "Nenhum registro encontrado. Finalizando ingestão."
            )

            break

        # ============================================
        # RAW PAYLOAD
        # ============================================

        for frentes in dados:

            frentes["raw_payload"] = json.dumps(
                frentes,
                ensure_ascii=False
            )

        # ============================================
        # DATAFRAME
        # ============================================

        spark_df = spark.createDataFrame(
            dados,
            schema=SCHEMA_FRENTES
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
        # ============================================

        salvar_delta(
            df=spark_df,
            tabela=TABELA_DESTINO,
            modo="append",
            particionar=True,
            colunas_particao=[
                "ano_ingestao",
                "mes_ingestao"
            ]
        )

        log_info(
            f"Página {pagina} gravada com sucesso."
        )

        pagina += 1

        # ============================================
        # RATE LIMIT
        # ============================================

        time.sleep(0.2)

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