# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/99_utils/common_utils

# COMMAND ----------

# DBTITLE 1,Cell 2
# ============================================================
# BRONZE_DEPUTADOS
# Projeto Final - Engenharia de Dados
# Camada Bronze | Ingerir dados brutos da API da Câmara dos Deputados
# mantendo fidelidade total ao payload original.
#
# ESTRATÉGIA DE CARGA:
# - 1ª Carga: OVERWRITE (cria tabela limpa)
# - Cargas subsequentes: MERGE/UPSERT por ID (captura atualizações)
#
# Motivo: Dados de deputados são master data que sofrem atualizações
# (partido, email, foto, status). MERGE permite capturar essas mudanças
# sem duplicação.
# ============================================================

# ============================================================
# 1. IMPORTS
# ============================================================

import time
import uuid
import json


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
MAX_PAGINAS = 100
MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

# ----------------------------
# Paginação
# ----------------------------

ITENS_POR_PAGINA = 100

# ----------------------------
# Pipeline
# ----------------------------

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())

# ----------------------------
# Delta Destination
# ----------------------------

TABELA_DESTINO = (
    "desafio_final_t2.bronze.bronze_deputados"
)


# ============================================================
# 3. CONTROLE DE ESCRITA
# ============================================================

primeira_carga = not spark.catalog.tableExists(
    TABELA_DESTINO
)

# ============================================================
# 4. INGESTÃO COM PAGINAÇÃO
# ============================================================

pagina = 1

while True:
    if pagina > MAX_PAGINAS:
            log_warning("Limite máximo de páginas atingido.")
            break
    try:

        log_info(
            f"Coletando página {pagina}"
        )

        params = {
            "pagina": pagina,
            "itens": ITENS_POR_PAGINA
        }

        # ============================================
        # REQUEST API
        # ============================================

        response_json = get_api_data(
            url=URL,
            params=params,
            max_retries=MAX_RETRIES,
            retry_delay=RETRY_DELAY,
            timeout=TIMEOUT
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
        # RAW PAYLOAD
        # ============================================

        for deputado in dados:

            deputado["raw_payload"] = json.dumps(
                deputado,
                ensure_ascii=False
            )

        # ============================================
        # DATAFRAME
        # ============================================

        spark_df = spark.createDataFrame(
            dados
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
        # Primeira carga: overwrite (limpa a tabela)
        # Subsequentes: MERGE (upsert por ID para capturar updates)
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
            # Cargas subsequentes: MERGE para capturar atualizações
            # Chave: ID do deputado (é única no sistema)
            salvar_delta(
                df=spark_df,
                tabela=TABELA_DESTINO,
                particionar=True,
                colunas_particao=[
                    "ano_ingestao",
                    "mes_ingestao"
                ],
                usar_merge=True,
                chaves_merge=["id"]
            )

        primeira_carga = False

        # ============================================
        # ESCRITA DELTA
        # ============================================

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
        # Falha intencional: preferimos falhar explicitamente
        # a continuar com dados incompletos na camada Bronze.
        raise
        

# ============================================================
# 4. FINALIZAÇÃO
# ============================================================

log_info(
    "Ingestão concluída com sucesso."
)
