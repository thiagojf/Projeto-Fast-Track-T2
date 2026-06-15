# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

# ============================================================
# BRONZE_EVENTOS
# Projeto Final - Engenharia de Dados
# Camada Bronze | Ingestão RAW API Câmara
# ============================================================

# ============================================================
# 1. IMPORTS
# ============================================================

import requests
import time
import uuid
import json

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import Row


# ============================================================
# 2. CONFIGURAÇÕES GERAIS
# ============================================================

# ----------------------------
# API
# ----------------------------

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
ENDPOINT = "/eventos"
URL = f"{BASE_URL}{ENDPOINT}"

# ----------------------------
# Retry / Timeout
# ----------------------------

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

# ----------------------------
# Pipeline
# ----------------------------

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())

# ----------------------------
# Delta Destination
# ----------------------------

TABELA_DESTINO = "desafio_final_t2.bronze.bronze_eventos"


# ============================================================
# 3. INGESTÃO COM PAGINAÇÃO
# ============================================================

# Lista que armazenará os registros
lista_eventos  = []

# Controle paginação
pagina = 1
itens_por_pagina = 100
DATA_INICIO = "2026-01-01"
DATA_FIM = "2026-12-31"

while True:

    try: 
        log_info(
            f"Coletando página {pagina}"
        )

        params = {
        "dataInicio": DATA_INICIO,
        "dataFim": DATA_FIM,
        "pagina": pagina,
        "itens": itens_por_pagina
        }

        # ============================================
        # REQUEST API
        # ============================================

        response_json = get_api_data(
            URL,
            params=params
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
        # LOOP REGISTROS RAW PAYLOAD
        # ============================================

        for eventos in dados:

            eventos["raw_payload"] = json.dumps(
                eventos,
                ensure_ascii=False
            )
        
        # ============================================
        # Na camada Bronze optamos por armazenar estruturas complexas serializadas em JSON para preservar fielmente a resposta da API e evitar problemas de inferência de schema. A normalização dessas estruturas ocorre posteriormente na camada Silver.
        # ============================================
        
        for evento in dados:

            evento["raw_payload"] = json.dumps(
                evento,
                ensure_ascii=False
            )

            for campo, valor in list(evento.items()):

                if isinstance(valor, (list, dict)):

                    evento[campo] = json.dumps(
                        valor,
                        ensure_ascii=False
                    )



        # ============================================
        # CRIAÇÃO DO DATAFRAME COM SCHEMA EXPLÍCITO
        # ============================================

        # Criei um StructType com todos os campos e seus tipos, garantindo que o Spark não precise inferir tipos de dados que contêm valores None
        schema = StructType([
            StructField("id", LongType(), True), #Ajustei de IntegerType para LongType para compatibilidade com o schema da tabela existente
            StructField("uri", StringType(), True),
            StructField("dataHoraInicio", StringType(), True),
            StructField("dataHoraFim", StringType(), True),
            StructField("situacao", StringType(), True),
            StructField("descricaoTipo", StringType(), True),
            StructField("descricao", StringType(), True),
            StructField("localExterno", StringType(), True),
            StructField("orgaos", StringType(), True),
            StructField("localCamara", StringType(), True),
            StructField("urlRegistro", StringType(), True),
            StructField("raw_payload", StringType(), True)
        ])

        # Transformei os dicionários em objetos Row antes de criar o DataFrame, o que torna a criação mais robusta no ambiente Spark Connect (serverless)
        rows = [Row(**evento) for evento in dados]

        spark_df = spark.createDataFrame(
                rows,
                schema=schema
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
        # Primeira execução = overwrite
        # Próximas execuções = append
        # ============================================
        if not spark.catalog.tableExists(TABELA_DESTINO):
            modo_escrita = "overwrite"
        else:
            modo_escrita = "append"
        
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


