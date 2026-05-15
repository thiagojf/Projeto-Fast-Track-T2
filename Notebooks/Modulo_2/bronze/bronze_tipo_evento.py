# Databricks notebook source
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
# 2. FUNÇÃO REQUEST COM RETRY
# ============================================================

def make_request(url, params=None):

    for tentativa in range(1, MAX_RETRIES + 1):

        try:
            response = requests.get(
                url=url,
                params=params,
                timeout=TIMEOUT
            )

            if response.status_code == 200:
                return response.json()

            print(f"[WARNING] Status Code: {response.status_code}")
            print(f"[WARNING] Response: {response.text[:500]}")

        except Exception as e:
            print(f"[ERROR] Tentativa {tentativa} falhou: {str(e)}")

        if tentativa < MAX_RETRIES:
            print(f"[INFO] Aguardando {RETRY_DELAY}s para retry...")
            time.sleep(RETRY_DELAY)

    raise Exception("Falha na requisição após múltiplas tentativas")


# ============================================================
# 3. INGESTÃO
# ============================================================

response_json = make_request(URL)

dados = response_json.get("dados", [])

if not dados:
    raise Exception("Nenhum tipo de evento retornado pela API.")

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
# 4. CRIAÇÃO SPARK DATAFRAME
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


# ============================================================
# 5. CAMPOS DE AUDITORIA
# ============================================================

spark_df = (
    spark_df
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("source_endpoint", F.lit(ENDPOINT))
    .withColumn("batch_id", F.lit(BATCH_ID))
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .withColumn("ano_ingestao", F.year(F.current_timestamp()))
    .withColumn("mes_ingestao", F.month(F.current_timestamp()))
)


# ============================================================
# 6. VALIDAÇÕES
# ============================================================

print(f"[INFO] Quantidade registros DataFrame: {spark_df.count()}")

spark_df.printSchema()

spark_df.show(20, truncate=False)


# ============================================================
# 7. ESCRITA DELTA BRONZE
# ============================================================

(
    spark_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ano_ingestao", "mes_ingestao")
    .saveAsTable(TABELA_DESTINO)
)


# ============================================================
# 8. VALIDAÇÃO FINAL
# ============================================================

spark.sql(f"""
SELECT
    COUNT(*) AS qtd_tipos_evento
FROM {TABELA_DESTINO}
""").show()