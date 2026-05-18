# ============================================================
# BRONZE_FRENTES_MEMBROS
# Camada Bronze | Ingestão RAW API Câmara
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
ENDPOINT_BASE = "/frentes"
ENDPOINT_MEMBROS_TEMPLATE = "/frentes/{id_frente}/membros"

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())

TABELA_ORIGEM_FRENTES = "desafio_final_T2.bronze.bronze_frentes"
TABELA_DESTINO = "desafio_final_T2.bronze.bronze_frentes_membros"


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

            print(f"[WARNING] Status Code {response.status_code} | URL: {url}")

        except Exception as e:
            print(f"[ERROR] Tentativa {tentativa} falhou | URL: {url} | Erro: {str(e)}")

        if tentativa < MAX_RETRIES:
            print(f"[INFO] Aguardando {RETRY_DELAY}s para retry...")
            time.sleep(RETRY_DELAY)

    raise Exception(f"Falha na requisição após múltiplas tentativas | URL: {url}")


# ============================================================
# 3. OBTER IDS DAS FRENTES JÁ INGESTADAS
# ============================================================

df_frentes = spark.table(TABELA_ORIGEM_FRENTES)

ids_frentes = [
    row["id"]
    for row in (
        df_frentes
        .select("id")
        .distinct()
        .collect()
    )
]

print(f"[INFO] Total de frentes para processar: {len(ids_frentes)}")


# ============================================================
# 4. INGESTÃO DOS MEMBROS POR FRENTE
# ============================================================

lista_frentes_membros = []

for id_frente in ids_frentes:

    endpoint_atual = f"/frentes/{id_frente}/membros"
    url_atual = f"{BASE_URL}{endpoint_atual}"

    print(f"[INFO] Coletando membros da frente {id_frente}")

    response_json = make_request(url=url_atual)

    dados = response_json.get("dados", [])

    if not dados:
        print(f"[WARNING] Nenhum membro encontrado para frente {id_frente}")
        continue

    for membro in dados:
        membro["id_frente"] = id_frente
        membro["source_endpoint_detail"] = endpoint_atual
        membro["raw_payload"] = json.dumps(membro, ensure_ascii=False)

        lista_frentes_membros.append(membro)

    print(
        f"[INFO] Frente {id_frente} | Registros acumulados: {len(lista_frentes_membros)}"
    )

    time.sleep(0.2)


# ============================================================
# 5. CRIAÇÃO SPARK DATAFRAME
# ============================================================

if not lista_frentes_membros:
    raise Exception("Nenhum membro de frente retornado pela API.")

json_strings = [
    json.dumps(registro, ensure_ascii=False)
    for registro in lista_frentes_membros
]

spark_df_raw = spark.createDataFrame(
    [(item,) for item in json_strings],
    ["json_string"]
)

spark_df = (
    spark_df_raw
    .select(
        F.from_json(
            F.col("json_string"),
            "struct<id:bigint,uri:string,nome:string,siglaPartido:string,uriPartido:string,siglaUf:string,idLegislatura:bigint,urlFoto:string,email:string,id_frente:bigint,source_endpoint_detail:string,raw_payload:string>"
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
    .withColumn("source_endpoint", F.lit(ENDPOINT_MEMBROS_TEMPLATE))
    .withColumn("batch_id", F.lit(BATCH_ID))
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .withColumn("ano_ingestao", F.year(F.current_timestamp()))
    .withColumn("mes_ingestao", F.month(F.current_timestamp()))
)




# ============================================================
# 7. ESCRITA DELTA BRONZE
# ============================================================

(
    spark_df.write
    .format("delta")
    .mode("append")
    .partitionBy("ano_ingestao", "mes_ingestao")
    .saveAsTable(TABELA_DESTINO)
)
