# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/99_utils/common_utils

# COMMAND ----------

# ============================================================
# BRONZE_ORGAOS_DETALHE
# Camada Bronze | Detalhamento dos Órgãos
# Endpoint: /orgaos/{id}
# Realizar a ingestão dos dados detalhados dos órgãos da Câmara dos Deputados.
# ============================================================

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================
# Importações
import json
import time
import uuid

from pyspark.sql import functions as F

# URL base da API de Dados Abertos da Câmara dos Deputados
BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

# Identificador único da execução para rastreabilidade
BATCH_ID = str(uuid.uuid4())

# Versão lógica do pipeline
PIPELINE_VERSION = "1.0"

## Tabelas de origem e destino
TABELA_ORIGEM_EVENTOS = "desafio_final_t2.bronze.bronze_orgaos"
TABELA_DESTINO = "desafio_final_t2.bronze.bronze_orgaos_detalhe"

# ============================================================
# 2. OBTÉM IDS DOS ORGAOS
# Recupera os IDs dos órgãos previamente carregados na camada Bronze para enriquecimento dos dados
# ============================================================

ids_orgaos = [

    row["id"]

    for row in (
        spark.table(TABELA_ORIGEM_EVENTOS)
        .select("id")
        .distinct()
        .collect()
    )
]

total_orgaos = len(ids_orgaos)

log_info(
    f"Total de órgãos para processar: {total_orgaos}"
)


# ============================================================
# 3. CONTROLE DE ERROS
# Acumula mensagens e informações sobre erros ocorridos ao longo da execução,
# permitindo geração de relatórios e análise posterior.
# ============================================================
lista_erros = []
PIPELINE_NAME = "bronze_orgaos_detalhe"


# ============================================================
# 4. PROCESSAMENTO DOS ORGAOS
# Para cada órgão consulta o endpoint de detalhe obtendo informações complementares
# ============================================================

for contador, id_orgao in enumerate(ids_orgaos, start=1):
    
    try:

        endpoint_atual = f"/orgaos/{id_orgao}"

        url_atual = (
            f"{BASE_URL}{endpoint_atual}"
        )

        log_info(
            f"Processando órgão "
            f"{contador}/{total_orgaos} "
            f"- ID {id_orgao}"
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
                f"Órgão {id_orgao} sem dados."
            )

            continue

        # ============================================
        # PREPARAÇÃO DOS DADOS
        # ============================================

        lista_orgaos = []

        dados["id_orgao"] = id_orgao

        dados["source_endpoint_detail"] = endpoint_atual

        dados["raw_payload"] = json.dumps(
            dados,
            ensure_ascii=False
        )

        lista_orgaos.append(dados)

        # ============================================
        # JSON -> DATAFRAME
        # ============================================

        json_strings = [

            json.dumps(
                registro,
                ensure_ascii=False
            )

            for registro in lista_orgaos

        ]

        spark_df_raw = spark.createDataFrame(
            [(item,) for item in json_strings],
            ["json_string"]
        )

        schema_orgaos_detalhes = """
        struct<
            apelido:string,
            casa:string,
            codTipoOrgao:string,
            dataFim:string,
            dataFimOriginal:string,
            dataInicio:string,
            dataInstalacao:string,
            id:string,
            id_orgao:string,
            nome:string,
            nomePublicacao:string,
            nomeResumido:string,
            sala:string,
            sigla:string,
            tipoOrgao:string,
            uri:string,
            urlWebsite:string,
            source_endpoint_detail:string,
            raw_payload:string
        >
        """

        spark_df = (

            spark_df_raw

            .select(
                F.from_json(
                    F.col("json_string"),
                    schema_orgaos_detalhes
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
        # Tabelas de referência/domínio estou usando modo="overwrite" porque a API devolve a fotografia atual.
        # ============================================

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

        log_info(
            f"Órgão {id_orgao} gravado com sucesso."
        )

        time.sleep(0.2)

    except Exception as e:

        log_error(
            f"Erro no órgão {id_orgao}: {str(e)}"
        )

        lista_erros.append({
            "id_orgao": id_orgao,
            "endpoint": endpoint_atual,
            "erro": str(e)

        })

        continue

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
# 5. RESUMO EXECUÇÃO
# ============================================================
log_info(
    "Ingestão concluída com sucesso."
)
