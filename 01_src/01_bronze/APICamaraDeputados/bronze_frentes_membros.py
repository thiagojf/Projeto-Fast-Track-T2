# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

# ============================================================
# BRONZE_FRENTES_MEMBROS
# Camada Bronze | Ingerir membros das frentes parlamentares preservando
# estrutura original da API com rastreabilidade total.
#
# ESTRATÉGIA DE CARGA:
# - 1ª Carga: OVERWRITE (cria tabela limpa)
# - Cargas subsequentes: MERGE/UPSERT (captura mudanças de composição)
#
# Motivo: Composição de frentes muda (entrada/saída de membros).
# MERGE permite capturar essas mudanças sem duplicação.
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
ENDPOINT = "/frentes/membros"

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())

TABELA_ORIGEM_FRENTES = "desafio_final_T2.bronze.bronze_frentes"
TABELA_DESTINO = "desafio_final_T2.bronze.bronze_frentes_membros"



# ============================================================
# 2. OBTER IDS DAS FRENTES JÁ INGESTADAS
# ============================================================

primeira_carga = not spark.catalog.tableExists(
    TABELA_DESTINO
)

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
# 3. INGESTÃO DOS MEMBROS POR FRENTE
# ============================================================

lista_frentes_membros = []

try:
    for id_frente in ids_frentes:

        endpoint_atual = f"/frentes/{id_frente}/membros"
        url_atual = f"{BASE_URL}{endpoint_atual}"

        log_info(
            f"Coletando membros da frente {id_frente}"
        )

        # ============================================
        # REQUEST API (sem parâmetros - API não aceita)
        # ============================================
        response_json = get_api_data(
            url=url_atual,
            params=None,
            max_retries=MAX_RETRIES,
            retry_delay=RETRY_DELAY,
            timeout=TIMEOUT
        )
        # ============================================
        # DADOS
        # ============================================
        dados = response_json.get("dados", [])
        
        if not dados:
            log_info(
                f"Nenhum membro encontrado para frente {id_frente}. Continuando..."
            )
            continue
            
        # ============================================
        # LOOP REGISTROS RAW PAYLOAD
        # ============================================
        for membro in dados:
            membro["id_frente"] = id_frente
            membro["source_endpoint_detail"] = endpoint_atual
            membro["raw_payload"] = json.dumps(membro, ensure_ascii=False)

            lista_frentes_membros.append(membro)

        print(
            f"[INFO] Frente {id_frente} | Registros acumulados: {len(lista_frentes_membros)}"
        )

        # ============================================
        # RATE LIMIT
        # ============================================
        time.sleep(0.2)

    # ============================================
    # DATAFRAME
    # ============================================
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
                "struct<id:bigint,uri:string,nome:string,siglaPartido:string,uriPartido:string,siglaUf:string,idLegislatura:bigint,urlFoto:string,email:string,titulo:string,codTitulo:bigint,dataInicio:string,dataFim:string,id_frente:bigint,source_endpoint_detail:string,raw_payload:string>"
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
    # DEFINIÇÃO DO MODO DE ESCRITA
    # Primeira carga: overwrite
    # Subsequentes: MERGE (upsert)
    # ============================================

    if primeira_carga:
        # Primeira carga: sobrescreve para começar limpo
        salvar_delta(
            df=spark_df,
            tabela=TABELA_DESTINO,
            modo="overwrite",
            particionar=True,
            colunas_particao=[
                "ano_ingestao",
                "mes_ingestao"
            ]
        )
    else:
        # Cargas subsequentes: MERGE para capturar composição dinâmica
        # Chaves: ID da frente + ID do deputado + data início
        salvar_delta(
            df=spark_df,
            tabela=TABELA_DESTINO,
            usar_merge=True,
            chaves_merge=["id_frente", "id", "dataInicio"],
            particionar=True,
            colunas_particao=[
                "ano_ingestao",
                "mes_ingestao"
            ]
        )

    primeira_carga = False
     
    # ============================================
    # ESCRITA DELTA
    # ============================================

    log_info(
        "Dados gravados com sucesso."
    )

except Exception as e:

    log_error(
        f"Erro na ingestão: {str(e)}"
    )

    raise
        

# ============================================================
# 4. FINALIZAÇÃO
# ============================================================

log_info(
    "Ingestão concluída com sucesso."
)


# COMMAND ----------

