# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------

# ============================================================
# BRONZE_EVENTOS_PRESENCA
# Camada Bronze | Ingerir dados de presença de deputados em eventos parlamentares
# mantendo fidelidade ao payload original da API da Câmara.
# Endpoint: /eventos/{id}/deputados
#
# ESTRATÉGIA DE CARGA:
# - 1ª Carga: OVERWRITE (cria tabela limpa)
# - Cargas subsequentes: MERGE/UPSERT por [id_evento, id] (evento + deputado)
#
# Motivo: Presença é um fato granular. Na primeira execução, sobrescrevemos para
# começar limpo. Em execuções posteriores, usamos MERGE para evitar duplicação
# ao reprocessar eventos (períodos com sobreposição).
# ============================================================

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

import json
import time
import uuid

from pyspark.sql import functions as F

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

TABELA_ORIGEM_EVENTOS = "desafio_final_t2.bronze.bronze_eventos"
TABELA_DESTINO = "desafio_final_t2.bronze.bronze_eventos_presenca"

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())

# ============================================================
# 2. CONTROLE DE PRIMEIRA CARGA
# ============================================================

primeira_carga = not spark.catalog.tableExists(
    TABELA_DESTINO
)

# ============================================================
# 3. OBTÉM IDS DOS EVENTOS
# Extraímos eventos da camada Bronze para garantir consistência
# ============================================================

ids_eventos = [

    row["id"]

    for row in (
        spark.table(TABELA_ORIGEM_EVENTOS)
        .select("id")
        .distinct()
        .collect()
    )
]

total_eventos = len(ids_eventos)

log_info(
    f"Total de eventos para processar: {total_eventos}"
)


# ============================================================
# 4. CONTROLE DE ERROS
# ============================================================

lista_erros = []


# ============================================================
# 5. PROCESSAMENTO DOS EVENTOS
# ============================================================

for contador, id_evento in enumerate(ids_eventos, start=1):

    try:

        endpoint_atual = f"/eventos/{id_evento}/deputados"

        url_atual = (
            f"{BASE_URL}{endpoint_atual}"
        )

        log_info(
            f"Processando evento "
            f"{contador}/{total_eventos} "
            f"- ID {id_evento}"
        )

        # ============================================
        # REQUEST API
        # ============================================

        response_json = get_api_data(
            url=url_atual,
            max_retries=MAX_RETRIES,
            retry_delay=RETRY_DELAY,
            timeout=TIMEOUT
        )

        dados = response_json.get(
            "dados",
            []
        )

        # ============================================
        # EVENTO SEM PRESENÇA
        # ============================================

        if not dados:

            log_warning(
                f"Evento {id_evento} sem presença."
            )

            continue

        # ============================================
        # PREPARAÇÃO DOS DADOS
        # ============================================

        lista_presenca = []

        for deputado in dados:

            deputado["id_evento"] = id_evento

            deputado[
                "source_endpoint_detail"
            ] = endpoint_atual

            deputado["raw_payload"] = json.dumps(
                deputado,
                ensure_ascii=False
            )

            lista_presenca.append(
                deputado
            )

        # ============================================
        # JSON -> DATAFRAME
        # ============================================

        json_strings = [

            json.dumps(
                registro,
                ensure_ascii=False
            )

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

        # ====================================================
        # ESCRITA DELTA
        # Primeira carga: overwrite para começar limpo
        # Subsequentes: MERGE para evitar duplicatas
        # ====================================================
        
        if primeira_carga:
            # Primeira carga: sobrescreve para começar limpo
            salvar_delta(
                df=spark_df,
                tabela=TABELA_DESTINO,
                modo="overwrite",
                particionar=True,
                colunas_particao=["ano_ingestao", "mes_ingestao"]
            )
        else:
            # Cargas subsequentes: MERGE para evitar duplicatas
            # Chaves: id_evento (evento) e id (deputado)
            salvar_delta(
                df=spark_df,
                tabela=TABELA_DESTINO,
                usar_merge=True,
                chaves_merge=["id_evento", "id"],
                particionar=True,
                colunas_particao=["ano_ingestao", "mes_ingestao"]
            )
        
        primeira_carga = False

        log_info(
            f"Evento {id_evento} gravado com sucesso."
        )

        time.sleep(0.2)

    except Exception as e:

        log_error(
            f"Erro no evento {id_evento}: {str(e)}"
        )

        lista_erros.append({

            "id_evento": id_evento,

            "endpoint": endpoint_atual,

            "erro": str(e)

        })

        continue


# ============================================================
# 4. FINALIZAÇÃO
# ============================================================

log_info(
    "Ingestão concluída com sucesso."
)