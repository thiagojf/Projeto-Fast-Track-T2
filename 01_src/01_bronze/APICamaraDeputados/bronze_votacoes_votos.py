# Databricks notebook source
# DBTITLE 1,Carrega as funções, variáveis e classes para uso no notebook atual.
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/99_utils/common_utils

# COMMAND ----------

# DBTITLE 1,Camada Bronze | Ingestão dos votos individuais dos deputados em votações
# ============================================================
# BRONZE_VOTACOES_VOTOS
# Camada Bronze | Ingerir votos individuais dos deputados em votações
# preservando granularidade máxima (fact table base).
# Endpoint: /votacoes/{id}/votos
#
# ESTRATÉGIA DE CARGA:
# - 1ª Carga: OVERWRITE (cria tabela limpa)
# - Cargas subsequentes: MERGE/UPSERT por [id_votacao, id] (deputado + votação)
#
# Motivo: Votos são fatos granulares. Na primeira execução, sobrescrevemos para
# começar limpo. Em execuções posteriores, usamos MERGE para evitar duplicação
# ao reprocessar votações (períodos com sobreposição).
# ============================================================

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

import json
import time
import uuid

from pyspark.sql import functions as F

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"


MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

PIPELINE_VERSION = "1.0"
BATCH_ID = str(uuid.uuid4())

## Tabelas de origem e destino
TABELA_ORIGEM = "desafio_final_t2.bronze.bronze_votacoes"
TABELA_DESTINO = "desafio_final_t2.bronze.bronze_votacoes_votos"

# ============================================================
# 2. CONTROLE DE PRIMEIRA CARGA
# ============================================================

primeira_carga = not spark.catalog.tableExists(
    TABELA_DESTINO
)

# ============================================================
# 3. OBTÉM IDS DAS VOTAÇÕES PARA PROCESSAR
# Recupera os IDs das votações previamente carregadas para enriquecimento dos dados com os votos individuais dos deputados.
# ============================================================

#id_votacao = ["2085970-127"]

id_votacao = [

    row["id"]

    for row in (
        spark.table(TABELA_ORIGEM)
        .select("id")
        .distinct()
        .collect()
    )
]

total_votacoes = len(id_votacao)

log_info(
    f"Total de Votações para processar: {total_votacoes}"
)


# ============================================================
# 4. CONTROLE DE ERROS
# Acumula mensagens e informações sobre erros ocorridos ao longo da execução,
# permitindo geração de relatórios e análise posterior.
# ============================================================
lista_erros = []
PIPELINE_NAME = "bronze_votacoes_votos"


# ============================================================
# 5. PROCESSAMENTO DOS VOTOS
# Consulta o endpoint de votos para cada votação retornando o posicionamento individual dos deputados
# ============================================================

for contador, id_votacao in enumerate(id_votacao, start=1):

    try:

        endpoint_atual = f"/votacoes/{id_votacao}/votos"

        url_atual = (
            f"{BASE_URL}{endpoint_atual}"
        )

        log_info(
            f"Processando Votação "
            f"{contador}/{total_votacoes} "
            f"- ID {id_votacao}"
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
        # VOTACAO SEM VOTO
        # ============================================

        if not dados:

            log_warning(
                f"Votacao {id_votacao} sem Voto."
            )

            continue

        # ============================================
        # PREPARAÇÃO DOS DADOS
        # ============================================

        lista_votos  = []

        # ====================================================
        # TRATAMENTO BRONZE
        # ====================================================
        for voto in dados:
            voto["id_votacao"] = id_votacao
            voto["source_endpoint_detail"] = endpoint_atual
            voto["raw_payload"] = json.dumps(voto, ensure_ascii=False)

            lista_votos.append(voto)

        # ============================================
        # JSON -> DATAFRAME
        # ============================================

        json_strings = [

            json.dumps(
                registro,
                ensure_ascii=False
            )

            for registro in lista_votos 

        ]

        spark_df_raw = spark.createDataFrame(
            [(item,) for item in json_strings],
            ["json_string"]
        )
        # ============================================================
        # 4.1. SCHEMA EXPLÍCITO
        # ============================================================
        schema_votos = """
        struct<
            dataRegistroVoto:string,
            tipoVoto:string,
            deputado_:struct<
                id:bigint,
                uri:string,
                nome:string,
                siglaPartido:string,
                uriPartido:string,
                siglaUf:string,
                idLegislatura:bigint,
                urlFoto:string,
                email:string
            >,
            id_votacao:string,
            source_endpoint_detail:string,
            raw_payload:string
        >
        """

        spark_df = (

            spark_df_raw

            .select(
                F.from_json(
                    F.col("json_string"),
                    schema_votos
                ).alias("dados")
            )

            .select("dados.*")
            # Extrai id do deputado para nível raiz (necessário para MERGE)
            .withColumn("id_deputado", F.col("deputado_.id"))

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
        # Primeira carga: overwrite para começar limpo
        # Subsequentes: MERGE para evitar duplicatas
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
            # Cargas subsequentes: MERGE para evitar duplicatas
            # Chaves: id_votacao (votação) e id (deputado)
            salvar_delta(
                df=spark_df,
                tabela=TABELA_DESTINO,
                usar_merge=True,
                chaves_merge=["id_votacao", "id_deputado"],
                particionar=True,
                colunas_particao=[
                    "ano_ingestao",
                    "mes_ingestao"
                ]
            )
        
        primeira_carga = False

        log_info(
            f"Votacao {id_votacao} gravada com sucesso."
        )

        time.sleep(0.2)

    except Exception as e:

        log_error(
            f"Erro no Votacao {id_votacao}: {str(e)}"
        )

        lista_erros.append({

            "id_votacao": id_votacao,
            "endpoint": endpoint_atual,
            "tipo_erro": type(e).__name__,
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
# 6. FINALIZAÇÃO
# ============================================================

log_info(
    "Ingestão concluída com sucesso."
)
