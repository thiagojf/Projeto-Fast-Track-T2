# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2.1/99_Utils/common_utils

# COMMAND ----------
# Databricks notebook source
# ============================================================
# BRONZE - EVENTOS
# Camada Bronze | Ingerir eventos parlamentares da API da Câmara dos Deputados
# preservando o payload original (raw data) com máxima fidelidade.
#
# ESTRATÉGIA DE CARGA:
# - 1ª Carga: OVERWRITE (cria tabela limpa)
# - Cargas subsequentes: MERGE/UPSERT por ID (permite correções)
#
# Motivo: Eventos com filtro de data podem ser reprocessados com periodos
# sobrepostos. MERGE evita duplicação quando janelas temporais se sobrepõem.
# ============================================================

import json
import time
import uuid

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
ENDPOINT = "/eventos"
URL = f"{BASE_URL}{ENDPOINT}"

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())

TABELA_DESTINO = "desafio_final_t2.bronze.bronze_eventos"

DATA_INICIO = "2026-01-01"
DATA_FIM = "2026-12-31"

ITENS_POR_PAGINA = 100


# ============================================================
# 2. CONTROLE DE ESCRITA
# ============================================================

primeira_carga = not spark.catalog.tableExists(
    TABELA_DESTINO
)


# ============================================================
# 3. INGESTÃO
# ============================================================

pagina = 1

while True:

    try:

        log_info(f"Coletando página {pagina}")

        params = {
            "dataInicio": DATA_INICIO,
            "dataFim": DATA_FIM,
            "pagina": pagina,
            "itens": ITENS_POR_PAGINA
        }

        # ====================================================
        # CHAMADA API
        # ====================================================
        response_json = get_api_data(
            URL,
            params=params,
            max_retries=MAX_RETRIES,
            retry_delay=RETRY_DELAY,
            timeout=TIMEOUT
        )

        dados = response_json.get("dados", [])

        if not dados:
            log_info("Fim da paginação (sem dados)")
            break

        # ====================================================
        # TRATAMENTO BRONZE (mínimo necessário)
        # ====================================================
        for evento in dados:

            # snapshot real do payload original
            evento["raw_payload"] = json.dumps(evento, ensure_ascii=False)

            # normalização leve apenas de estruturas complexas
            for campo, valor in list(evento.items()):
                if isinstance(valor, (list, dict)):
                    evento[campo] = json.dumps(valor, ensure_ascii=False)

        # ====================================================
        # SCHEMA BRONZE (tolerante a mudanças)
        # ====================================================
        schema = StructType([
            StructField("id", LongType(), True),
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

        rows = [Row(**evento) for evento in dados]

        spark_df = spark.createDataFrame(rows, schema=schema)

        # ====================================================
        # AUDITORIA PIPELINE
        # ====================================================
        spark_df = adicionar_auditoria(
            df=spark_df,
            endpoint=ENDPOINT,
            batch_id=BATCH_ID,
            pipeline_version=PIPELINE_VERSION
        )

        # ====================================================
        # DEFINIÇÃO DO MODO DE ESCRITA
        # Primeira carga: overwrite
        # Subsequentes: MERGE (upsert por ID)
        # ====================================================

        if primeira_carga:
            # Primeira carga: sobrescreve para começar limpo
            modo_escrita = "overwrite"
            usar_merge = False
        else:
            # Cargas subsequentes: MERGE para evitar duplicatas
            # de períodos sobrepostos
            modo_escrita = None
            usar_merge = True

        # ====================================================
        # ESCRITA BRONZE (com suporte a MERGE)
        # ====================================================
        if usar_merge:
            salvar_delta(
                df=spark_df,
                tabela=TABELA_DESTINO,
                usar_merge=True,
                chaves_merge=["id"],
                particionar=True,
                colunas_particao=["ano_ingestao", "mes_ingestao"]
            )
        else:
            salvar_delta(
                df=spark_df,
                tabela=TABELA_DESTINO,
                modo=modo_escrita,
                particionar=True,
                colunas_particao=["ano_ingestao", "mes_ingestao"]
            )

        primeira_carga = False

        log_info(f"Página {pagina} gravada com sucesso")

        pagina += 1
        time.sleep(0.2)

    except Exception as e:

        log_error(f"Erro na página {pagina}: {str(e)}")
        raise


# ============================================================
# FINALIZAÇÃO
# ============================================================

log_info("Ingestão de eventos concluída com sucesso")


