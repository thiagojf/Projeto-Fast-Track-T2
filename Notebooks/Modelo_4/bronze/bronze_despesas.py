# Databricks notebook source
# ============================================================
# BRONZE_DESPESAS
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
ENDPOINT_BASE = "/deputados"
ENDPOINT_DESPESAS_TEMPLATE = "/deputados/{id}/despesas"

MAX_RETRIES = 3
RETRY_DELAY = 5
TIMEOUT = 60
ANOS_REFERENCIA = [2023, 2024, 2025, 2026, 2027]
ITENS_POR_PAGINA = 50

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())

TABELA_ORIGEM_DEPUTADOS = "desafio_final_T2.silver.dim_deputado"
TABELA_DESTINO = "desafio_final_T2.bronze.bronze_despesas"

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

#ids_deputados = [204379]

ids_deputados = [
    row["nk_deputado"]
    for row in (
        spark.table(TABELA_ORIGEM_DEPUTADOS)
        .filter(F.col("is_current") == True)
        .select("nk_deputado")
        .distinct()
        #.limit(5)  # teste inicial
        .collect()
    )
]

print(f"[INFO] Total de deputados para processar: {len(ids_deputados)}")


# ============================================================
# 4. INGESTÃO DOS MEMBROS POR FRENTE
# ============================================================

lista_despesas = []

for id_deputado in ids_deputados:

    for ano_referencia in ANOS_REFERENCIA:

        pagina = 1

        endpoint_atual = f"/deputados/{id_deputado}/despesas"
        url_atual = f"{BASE_URL}{endpoint_atual}"

        while True:

            params = {
                "ano": ano_referencia,
                "pagina": pagina,
                "itens": ITENS_POR_PAGINA
            }

            print(
                f"[INFO] Deputado {id_deputado} | "
                f"Ano {ano_referencia} | Página {pagina}"
            )

            response_json = make_request(
                url=url_atual,
                params=params
            )

            dados = response_json.get("dados", [])

            if not dados:
                print(
                    f"[INFO] Fim deputado {id_deputado} | "
                    f"Ano {ano_referencia}"
                )
                break

            for despesa in dados:

                despesa["nk_deputado"] = id_deputado
                despesa["ano_referencia"] = ano_referencia
                despesa["source_endpoint_detail"] = endpoint_atual
                despesa["raw_payload"] = json.dumps(
                    despesa,
                    ensure_ascii=False
                )

                lista_despesas.append(despesa)

            print(f"[INFO] Registros acumulados: {len(lista_despesas)}")

            pagina += 1

            time.sleep(0.2)


# ============================================================
# 5. CRIAÇÃO SPARK DATAFRAME
# ============================================================

if not lista_despesas:
    raise Exception(
        f"Nenhuma despesa retornada pela API para os anos {ANOS_REFERENCIA}."
    )

json_strings = [
    json.dumps(registro, ensure_ascii=False)
    for registro in lista_despesas
]

spark_df_raw = spark.createDataFrame(
    [(item,) for item in json_strings],
    ["json_string"]
)

schema_despesas = """
struct<
    ano:int,
    mes:int,
    tipoDespesa:string,
    codDocumento:bigint,
    tipoDocumento:string,
    codTipoDocumento:int,
    dataDocumento:string,
    numDocumento:string,
    valorDocumento:double,
    urlDocumento:string,
    nomeFornecedor:string,
    cnpjCpfFornecedor:string,
    valorLiquido:double,
    valorGlosa:double,
    numRessarcimento:string,
    codLote:bigint,
    parcela:int,
    nk_deputado:bigint,
    ano_referencia:int,
    source_endpoint_detail:string,
    raw_payload:string
>
"""

spark_df = (
    spark_df_raw
    .select(F.from_json(F.col("json_string"), schema_despesas).alias("dados"))
    .select("dados.*")
)


# ============================================================
# 7. CAMPOS DE AUDITORIA
# ============================================================

spark_df = (
    spark_df
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("source_endpoint", F.lit(ENDPOINT_DESPESAS_TEMPLATE))
    .withColumn("batch_id", F.lit(BATCH_ID))
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    .withColumn("ano_ingestao", F.year(F.current_timestamp()))
    .withColumn("mes_ingestao", F.month(F.current_timestamp()))
)


# ============================================================
# 8. VALIDAÇÕES
# ============================================================

print(f"[INFO] Quantidade registros DataFrame: {spark_df.count()}")

spark_df.printSchema()

spark_df.show(5, truncate=False)


# ============================================================
# 9. ESCRITA DELTA BRONZE
# ============================================================

(
    spark_df.write
    .format("delta")
    .mode("append")
    .partitionBy("ano_ingestao", "mes_ingestao")
    .saveAsTable(TABELA_DESTINO)
)