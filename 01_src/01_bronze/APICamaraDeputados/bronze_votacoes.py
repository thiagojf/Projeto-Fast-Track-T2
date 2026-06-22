# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/99_utils/common_utils

# COMMAND ----------

# ============================================================
# BRONZE_VOTACOES
# Camada Bronze | Votações da Câmara
# Endpoint: /votacoes
# 
# ESTRATÉGIA DE CARGA:
# - 1ª Carga: OVERWRITE (cria tabela limpa)
# - Cargas subsequentes: MERGE/UPSERT por ID (evita duplicatas)
# 
# Motivo: Votações são fatos históricos. Na primeira execução,
# sobrescrevemos para começar limpo. Em execuções posteriores,
# usamos MERGE para evitar duplicação quando processamos períodos
# que podem ter sobreposição de datas.
# ============================================================

# ============================================================
# 1. IMPORTS
# ============================================================

import json
import time
import uuid

from pyspark.sql import functions as F

# ============================================================
# 2. CONFIGURAÇÕES
# ============================================================

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
ENDPOINT = "/votacoes"
URL = f"{BASE_URL}{ENDPOINT}"

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())

TABELA_DESTINO = "desafio_final_t2.bronze.bronze_votacoes"

# Acumula mensagens e informações sobre erros ocorridos ao longo da execução,
# permitindo geração de relatórios e análise posterior.
# ============================================================
lista_erros = []
PIPELINE_NAME = "bronze_votacoes"

# ============================================================
# 3. CONTROLE DE ESCRITA
# ============================================================

primeira_carga = not spark.catalog.tableExists(
    TABELA_DESTINO
)

# ============================================================
# 4. PAGINAÇÃO
# ============================================================

pagina = 1
itens_por_pagina = 100

while True:

    try:

        log_info(
            f"Coletando página {pagina}"
        )

        params = {
            "pagina": pagina,
            "itens": itens_por_pagina
        }

        # ====================================================
        # REQUEST API
        # ====================================================

        response_json = get_api_data(
            url=URL,
            params=params,
            max_retries=MAX_RETRIES,
            retry_delay=RETRY_DELAY,
            timeout=TIMEOUT
        )

        dados = response_json.get(
            "dados",
            []
        )

        # ====================================================
        # CONDIÇÃO DE PARADA
        # ====================================================

        if not dados:

            log_info(
                "Nenhum registro encontrado. Finalizando ingestão."
            )

            break

        # ====================================================
        # PREPARAÇÃO DOS DADOS
        # ====================================================

        lista_votacoes = []

        for votacao in dados:

            votacao[
                "source_endpoint_detail"
            ] = ENDPOINT

            votacao["raw_payload"] = json.dumps(
                votacao,
                ensure_ascii=False
            )

            lista_votacoes.append(
                votacao
            )

        # ====================================================
        # JSON -> DATAFRAME
        # Evita erro de inferência do Spark
        # ====================================================

        json_strings = [

            json.dumps(
                registro,
                ensure_ascii=False
            )

            for registro in lista_votacoes
        ]

        spark_df_raw = spark.createDataFrame(
            [(item,) for item in json_strings],
            ["json_string"]
        )

        # ====================================================
        # SCHEMA EXPLÍCITO
        # ====================================================

        schema_votacoes = """
        struct<
            id:string,
            uri:string,
            data:string,
            dataHoraRegistro:string,
            siglaOrgao:string,
            uriOrgao:string,
            uriEvento:string,
            proposicaoObjeto:string,
            uriProposicaoObjeto:string,
            descricao:string,
            aprovacao:int,
            source_endpoint_detail:string,
            raw_payload:string
        >
        """

        spark_df = (

            spark_df_raw

            .select(
                F.from_json(
                    F.col("json_string"),
                    schema_votacoes
                ).alias("dados")
            )

            .select("dados.*")

        )

        # ====================================================
        # AUDITORIA
        # ====================================================

        spark_df = adicionar_auditoria(
            df=spark_df,
            endpoint=ENDPOINT,
            batch_id=BATCH_ID,
            pipeline_version=PIPELINE_VERSION
        )

        # ====================================================
        # DEFINIÇÃO DO MODO DE ESCRITA
        # Primeira carga: overwrite (limpa a tabela)
        # Subsequentes: MERGE (upsert por ID para evitar duplicatas)
        # ====================================================

        if primeira_carga:
            # Primeira carga: sobrescreve para começar limpo
            modo_escrita = "overwrite"
            usar_merge = False
            
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
        else:
            # Cargas subsequentes: MERGE para evitar duplicatas
            # Chave: ID da votação (é única no sistema)
            salvar_delta(
                df=spark_df,
                tabela=TABELA_DESTINO,
                usar_merge=True,
                chaves_merge=["id"],
                particionar=True,
                colunas_particao=[
                    "ano_ingestao",
                    "mes_ingestao"
                ]
            )

        primeira_carga = False

        log_info(
            f"Página {pagina} gravada com sucesso."
        )

        pagina += 1

        time.sleep(0.2)

    except Exception as e:

        log_error(
            f"Erro na página {pagina}: {str(e)}"
        )

        lista_erros.append({

            "pagina": pagina,
            "endpoint": ENDPOINT,
            "tipo_erro": type(e).__name__,
            "erro": str(e)

        })
        raise

# ============================================================
# Gravação de logs de erro
# ============================================================

if lista_erros:
    df_erros = spark.createDataFrame(lista_erros)
    df_erros = (
        df_erros
        .withColumn("data_execucao",F.current_timestamp())
        .withColumn("pipeline",F.lit(PIPELINE_NAME))
        .withColumn("batch_id",F.lit(BATCH_ID))
        .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
    )
    salvar_delta(
        df=df_erros,
        tabela="desafio_final_t2.audit.erros_api",
        modo="append"
    )
    log_info(
        f"{len(lista_erros)} erros gravados na auditoria."
    )

# ============================================================
# 5. FINALIZAÇÃO
# ============================================================

log_info(
    "Ingestão concluída com sucesso."
)
