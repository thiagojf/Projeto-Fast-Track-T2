# Databricks notebook source
# ============================================================
# BRONZE_DEPUTADOS
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


# ============================================================
# 2. CONFIGURAÇÕES GERAIS
# ============================================================

# ----------------------------
# API
# ----------------------------

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
ENDPOINT = "/deputados"

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

TABELA_DESTINO = "desafio_final_T2.bronze.bronze_deputados"


# ============================================================
# 3. FUNÇÃO REQUEST COM RETRY
# ============================================================

def make_request(url, params=None):

    for tentativa in range(1, MAX_RETRIES + 1):

        try:

            response = requests.get(
                url=url,
                params=params,
                timeout=TIMEOUT
            )

            # ============================================
            # SUCESSO
            # ============================================

            if response.status_code == 200:
                return response.json()

            print(
                f"[WARNING] Status Code: {response.status_code}"
            )

        except Exception as e:

            print(
                f"[ERROR] Tentativa {tentativa} falhou: {str(e)}"
            )

        # ============================================
        # RETRY
        # ============================================

        if tentativa < MAX_RETRIES:

            print(
                f"[INFO] Aguardando {RETRY_DELAY}s para retry..."
            )

            time.sleep(RETRY_DELAY)

    # ============================================
    # FALHA FINAL
    # ============================================

    raise Exception(
        "Falha na requisição após múltiplas tentativas"
    )


# ============================================================
# 4. INGESTÃO COM PAGINAÇÃO
# ============================================================

# Lista que armazenará os registros
lista_deputados = []

# Controle paginação
pagina = 1
itens_por_pagina = 100

while True:

    print(f"[INFO] Coletando página {pagina}")

    params = {
        "pagina": pagina,
        "itens": itens_por_pagina
    }

    # ============================================
    # REQUEST API
    # ============================================

    response_json = make_request(
        URL,
        params=params
    )

    # ============================================
    # DADOS
    # ============================================

    dados = response_json.get("dados", [])

    # ============================================
    # CONDIÇÃO PARADA
    # ============================================

    if not dados:

        print(
            "[INFO] Nenhum registro encontrado. Finalizando ingestão."
        )

        break

    # ============================================
    # LOOP REGISTROS
    # ============================================

    for deputado in dados:

        # ----------------------------------------
        # Payload RAW
        # ----------------------------------------

        deputado["raw_payload"] = json.dumps(
            deputado,
            ensure_ascii=False
        )

        # ----------------------------------------
        # Adiciona na lista
        # ----------------------------------------

        lista_deputados.append(deputado)

    print(
        f"[INFO] Registros acumulados: {len(lista_deputados)}"
    )

    # Próxima página
    pagina += 1

    # ============================================
    # RATE LIMITING SIMPLES
    # ============================================

    time.sleep(0.2)


# ============================================================
# 5. CRIAÇÃO SPARK DATAFRAME
# ============================================================

spark_df = spark.createDataFrame(lista_deputados)


# ============================================================
# 6. PADRONIZAÇÃO NOMES COLUNAS
# ============================================================

spark_df = (

    spark_df

    .withColumnRenamed(
        "siglaPartido",
        "sigla_partido"
    )

    .withColumnRenamed(
        "uriPartido",
        "uri_partido"
    )

    .withColumnRenamed(
        "siglaUf",
        "sigla_uf"
    )

    .withColumnRenamed(
        "idLegislatura",
        "id_legislatura"
    )

    .withColumnRenamed(
        "urlFoto",
        "url_foto"
    )

)


# ============================================================
# 7. CAMPOS AUDITORIA
# ============================================================

spark_df = (

    spark_df

    # --------------------------------------------
    # Data ingestão
    # --------------------------------------------

    .withColumn(
        "ingested_at",
        F.current_timestamp()
    )

    # --------------------------------------------
    # Data atualização
    # --------------------------------------------

    .withColumn(
        "updated_at",
        F.current_timestamp()
    )

    # --------------------------------------------
    # Endpoint origem
    # --------------------------------------------

    .withColumn(
        "source_endpoint",
        F.lit(ENDPOINT)
    )

    # --------------------------------------------
    # Batch execução
    # --------------------------------------------

    .withColumn(
        "batch_id",
        F.lit(BATCH_ID)
    )

    # --------------------------------------------
    # Versão pipeline
    # --------------------------------------------

    .withColumn(
        "pipeline_version",
        F.lit(PIPELINE_VERSION)
    )

    # --------------------------------------------
    # Particionamento temporal
    # --------------------------------------------

    .withColumn(
        "ano_ingestao",
        F.year(F.current_timestamp())
    )

    .withColumn(
        "mes_ingestao",
        F.month(F.current_timestamp())
    )

)


# ============================================================
# 8. VALIDAÇÕES
# ============================================================

print(
    f"[INFO] Quantidade registros DataFrame: {spark_df.count()}"
)

spark_df.printSchema()

spark_df.show(
    5,
    truncate=False
)


# ============================================================
# 9. ESCRITA DELTA BRONZE
# ============================================================

(
    spark_df.write

    .format("delta")

    # Bronze deve preservar histórico
    .mode("append")

    .partitionBy(
        "ano_ingestao",
        "mes_ingestao"
    )

    .saveAsTable(TABELA_DESTINO)
)


# ============================================================
# 10. VALIDAÇÃO FINAL DELTA
# ============================================================

spark.sql(f"""

SELECT
    *
FROM {TABELA_DESTINO}

""").show()