# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

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
pagina = 1

while True:

    try:
        for id_evento in ids_eventos:

            endpoint_atual = f"/eventos/{id_evento}/deputados"
            url_atual = f"{BASE_URL}{endpoint_atual}"

            log_info(
            f"[INFO] Coletando presença do evento {id_evento}"
            )
            # ============================================
            # REQUEST API
            # ============================================

            try:
                response_json = get_api_data(url=url_atual)

            except Exception as e:
                print(f"[ERROR] Falha definitiva evento {id_evento}: {str(e)}")

                lista_erros.append({
                    "id_evento": id_evento,
                    "endpoint": endpoint_atual,
                    "erro": str(e)
                })

                continue
            # ============================================
            # DADOS
            # ============================================
            dados = response_json.get("dados", [])
            # ============================================
            # CONDIÇÃO DE PARADA
            # ============================================
            if not dados:
                log_info(
               "[WARNING] Nenhum deputado retornado para evento {id_evento}"
               )
                continue
            # ============================================
            # RAW PAYLOAD
            # ============================================
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
        # ============================================
        # CRIAÇÃO DO DATAFRAME
        # ============================================

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
