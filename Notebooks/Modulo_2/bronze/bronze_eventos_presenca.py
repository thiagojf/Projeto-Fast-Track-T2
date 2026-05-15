# Databricks notebook source
# ============================================================
# BRONZE_EVENTOS_PRESENCA
# Camada Bronze | Presença de deputados por evento
# Endpoint: /eventos/{id}/deputados
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

TABELA_ORIGEM_EVENTOS = "desafio_final_t2.bronze.bronze_eventos"
TABELA_DESTINO = "desafio_final_t2.bronze.bronze_eventos_presenca"

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())


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
            print(f"[ERROR] Tentativa {tentativa} falhou | URL: {url} | Erro: {str(e)}")

        if tentativa < MAX_RETRIES:
            print(f"[INFO] Aguardando {RETRY_DELAY}s para retry...")
            time.sleep(RETRY_DELAY)

    raise Exception(f"Falha na requisição após múltiplas tentativas | URL: {url}")


# ============================================================
# 3. OBTER IDS DOS EVENTOS
# ============================================================

ids_eventos = [
    row["id"]
    for row in (
        spark.table(TABELA_ORIGEM_EVENTOS)
        .select("id")
        .distinct()
        #.limit(5)  # teste inicial; remova ou aumente depois
        .collect()
    )
]

print(f"[INFO] Total de eventos para processar: {len(ids_eventos)}")


# ============================================================
# 4. INGESTÃO DE PRESENÇA POR EVENTO
# ============================================================

lista_presenca = []
lista_erros = []

for id_evento in ids_eventos:

    endpoint_atual = f"/eventos/{id_evento}/deputados"
    url_atual = f"{BASE_URL}{endpoint_atual}"

    print(f"[INFO] Coletando presença do evento {id_evento}")

    try:
        response_json = make_request(url=url_atual)

    except Exception as e:
        print(f"[ERROR] Falha definitiva evento {id_evento}: {str(e)}")

        lista_erros.append({
            "id_evento": id_evento,
            "endpoint": endpoint_atual,
            "erro": str(e)
        })

        continue

    dados = response_json.get("dados", [])

    if not dados:
        print(f"[WARNING] Nenhum deputado retornado para evento {id_evento}")
        continue

    for deputado in dados:

        deputado["id_evento"] = id_evento
        deputado["source_endpoint_detail"] = endpoint_atual
        deputado["raw_payload"] = json.dumps(deputado, ensure_ascii=False)

        lista_presenca.append(deputado)

    print(
        f"[INFO] Evento {id_evento} | Deputados: {len(dados)} | "
        f"Acumulado: {len(lista_presenca)}"
    )

    time.sleep(0.2)


# ============================================================
# 5. CRIAÇÃO SPARK DATAFRAME
# Compatível com Databricks Serverless
# ============================================================

if not lista_presenca:
    raise Exception("Nenhuma presença retornada pela API.")

json_strings = [
    json.dumps(registro, ensure_ascii=False)
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


# ============================================================
# 6. CAMPOS DE AUDITORIA
# ============================================================

spark_df = (
    spark_df
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("source_endpoint", F.lit("/eventos/{id}/deputados"))
    .withColumn("batch_id", F.lit(BATCH_ID))
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .withColumn("ano_ingestao", F.year(F.current_timestamp()))
    .withColumn("mes_ingestao", F.month(F.current_timestamp()))
)


# ============================================================
# 7. VALIDAÇÕES
# ============================================================

print(f"[INFO] Quantidade registros DataFrame: {spark_df.count()}")
print(f"[WARNING] Total de erros: {len(lista_erros)}")

spark_df.printSchema()
spark_df.show(5, truncate=False)


# ============================================================
# 8. ESCRITA DELTA BRONZE
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
# 9. VALIDAÇÃO FINAL
# ============================================================

spark.sql(f"""
SELECT
    COUNT(*) AS qtd_linhas,
    COUNT(DISTINCT id_evento) AS qtd_eventos,
    COUNT(DISTINCT id) AS qtd_deputados
FROM {TABELA_DESTINO}
""").show()