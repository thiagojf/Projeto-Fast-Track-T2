# Databricks notebook source
# MAGIC %run /Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/99_utils/common_utils

# COMMAND ----------

# ============================================================
# BRONZE DESPESAS POR DEPUTADO
# Projeto Final - Engenharia de Dados
# Camada Bronze | Ingerir dados de despesas parlamentares da API da Câmara dos Deputados
# mantendo fidelidade ao payload original (camada Bronze).
#
# ESTRATÉGIA DE CARGA:
# - 1ª Carga: OVERWRITE (cria tabela limpa)
# - Cargas subsequentes: MERGE/UPSERT por [nk_deputado, ano_referencia, codDocumento]
#
# Motivo: Despesas podem ser corrigidas/atualizadas pela API. Na primeira execução,
# sobrescrevemos para começar limpo. Em execuções posteriores, usamos MERGE para
# evitar duplicação ao reprocessar períodos (anos/meses sobrepostos).
# ============================================================

import requests
import time
import uuid
import json

from pyspark.sql import functions as F
from pyspark.sql.functions import sha2, concat_ws, coalesce, lit

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

# Endpoint dinâmico por deputado
ENDPOINT_BASE = "/deputados"
ENDPOINT_TEMPLATE = "/deputados/{id}/despesas"

# Configurações de resiliência da API
MAX_RETRIES = 3
RETRY_DELAY = 5
TIMEOUT = 60

# Anos analisados (janela temporal da extração)
ANOS_REFERENCIA = [2023, 2024, 2025]

# Controle de paginação da API
ITENS_POR_PAGINA = 50

# Governança do pipeline
PIPELINE_VERSION = "1.1"
BATCH_ID = str(uuid.uuid4())

# Tabela destino Bronze
TABELA_ORIGEM_DEPUTADOS = "desafio_final_t2.bronze.bronze_deputados"
TABELA_DESTINO = "desafio_final_T2.bronze.bronze_despesas"

# Acumula mensagens e informações sobre erros ocorridos ao longo da execução,
# permitindo geração de relatórios e análise posterior.
lista_erros = []

# ============================================================
# 2. OBTER IDS DOS DEPUTADOS
# Extraímos os deputados da base Bronze já existente
# para garantir consistência entre pipelines
# ============================================================

#ids_deputados = [204379]
ids_deputados = [
    row["id"]
    for row in (
        spark.table(TABELA_ORIGEM_DEPUTADOS)
        .select("id")
        .distinct()
        .limit(5)
        .collect()
    )
]

print(f"[INFO] Total de deputados para processar: {len(ids_deputados)}")


# ============================================================
# 3. INGESTÃO DAS DESPESAS POR DEPUTADO
# ============================================================

try:

    log_info(
        "Iniciando ingestão de despesas"
    )
    # ====================================================
    # Verifica se a tabela já existe na primeira execução não existindo overwrite (cria a tabela).
    # Demais execuções usa append para evitar duplicidade
    # ====================================================
    primeira_carga = not spark.catalog.tableExists(
        TABELA_DESTINO
    )

    for id_deputado in ids_deputados:
        # ============================================
        # Lista exclusiva do deputado atual quando terminar o processamento deste deputado os dados já serão gravados.
        # Se ocorrer erro no próximo deputado, os anteriores já estarão persistidos.
        # ============================================
        lista_despesas_deputado = []

        for ano_referencia in ANOS_REFERENCIA:

            pagina = 1

            endpoint_atual = (
                f"/deputados/{id_deputado}/despesas"
            )

            url_atual = (
                f"{BASE_URL}{endpoint_atual}"
            )

            while True:

                params = {
                    "ano": ano_referencia,
                    "pagina": pagina,
                    "itens": ITENS_POR_PAGINA
                }

                log_info(
                    f"Deputado {id_deputado} | "
                    f"Ano {ano_referencia} | "
                    f"Página {pagina}"
                )

                # ============================================
                # REQUEST API
                # ============================================

                response_json = get_api_data(
                    url=url_atual,
                    params=params
                )

                # ============================================
                # DADOS
                # ============================================

                dados = response_json.get(
                    "dados",
                    []
                )

                # ============================================
                # FIM PAGINAÇÃO
                # ============================================

                if not dados:

                    log_info(
                        f"Fim deputado {id_deputado} | "
                        f"Ano {ano_referencia}"
                    )

                    break

                # ============================================
                # RAW PAYLOAD
                # ============================================

                for despesa in dados:

                    despesa["nk_deputado"] = id_deputado

                    despesa["ano_referencia"] = (
                        ano_referencia
                    )

                    despesa[
                        "source_endpoint_detail"
                    ] = endpoint_atual

                    despesa["raw_payload"] = json.dumps(
                        despesa,
                        ensure_ascii=False
                    )
                    # ============================================
                    # Armazena somente despesas do deputado atual, lista criada no inicio
                    # ============================================
                    lista_despesas_deputado.append(
                        despesa
                    )

                log_info(
                    f"Registros acumulados deputado "
                    f"{id_deputado}: "
                    f"{len(lista_despesas_deputado)}"
                )

                pagina += 1

                time.sleep(0.2)

        # ====================================================
        # Valida a lista de despesas por dputado e retorna se temos dados para persistir
        # não encontrano retorna um log_warning mas continua o processo
        # para processar as despeas do proximo deputado
        # ====================================================

        if not lista_despesas_deputado:

            log_warning(
                f"Nenhuma despesa encontrada "
                f"para deputado {id_deputado}"
            )

            continue

        # ====================================================
        # CRIAÇÃO DATAFRAME
        # ====================================================

        json_strings = [

            json.dumps(
                registro,
                ensure_ascii=False
            )

            for registro in lista_despesas_deputado
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

            .select(

                F.from_json(
                    F.col("json_string"),
                    schema_despesas
                ).alias("dados")

            )

            .select("dados.*")

        )

        # Gera a chave substituta (sk_despesa) aplicando SHA-256 sobre a
        # concatenação dos campos que identificam unicamente uma despesa.
        # Valores nulos são substituídos por string vazia para garantir
        # consistência na geração do hash.
        spark_df = spark_df.withColumn(
            "sk_despesa",
            sha2(
                concat_ws("|",
                    coalesce(F.col("nk_deputado").cast("string"), lit("")),
                    coalesce(F.col("codDocumento").cast("string"), lit("")),
                    coalesce(F.col("numDocumento"), lit("")),
                    coalesce(F.date_format("dataDocumento", "yyyy-MM-dd"), lit(""))
                    coalesce(F.format_number("valorLiquido", 2), lit("")),
                    coalesce(F.col("codLote").cast("string"), lit(""))
                ),
                256
            )
        )

        # ====================================================
        # AUDITORIA
        # ====================================================

        spark_df = adicionar_auditoria(
            df=spark_df,
            endpoint=ENDPOINT_BASE,
            batch_id=BATCH_ID,
            pipeline_version=PIPELINE_VERSION
        )

        # ====================================================
        # DEFINIÇÃO DO MODO DE ESCRITA
        # Primeira carga: overwrite (limpa a tabela)
        # Subsequentes: MERGE (upsert por chave para evitar duplicatas)
        # ====================================================

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
            # Chaves: deputado (nk_deputado), ano de referência, e código do documento
            # Esta combinação identifica uma despesa única
            salvar_delta(
                df=spark_df,
                tabela=TABELA_DESTINO,
                usar_merge=True,
                chaves_merge=["sk_despesa"],
                particionar=True,
                colunas_particao=[
                    "ano_ingestao",
                    "mes_ingestao"
                ]
            )
        # ====================================================
        # Após a primeira gravação a tabela já existe e a varivael primeira_carga passa aser FALSE e as próximas gravações serão MERGE.
        # ====================================================
        primeira_carga = False

        log_info(
            f"Deputado {id_deputado} gravado com "
            f"{len(lista_despesas_deputado)} registros."
        )

except Exception as e:

    log_error(
        f"Erro durante ingestão: {str(e)}"
    )

    lista_erros.append({

       "id_deputado": id_deputado,
        "endpoint": endpoint_atual,
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
        .withColumn("pipeline",F.lit("bronze_despesas"))
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
# 4. FINALIZAÇÃO
# ============================================================

log_info(
    "Ingestão concluída com sucesso."
)
