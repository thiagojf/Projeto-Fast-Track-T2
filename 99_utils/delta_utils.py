# Databricks notebook source
# Databricks notebook source
import uuid
from pyspark.sql import functions as F
from delta.tables import DeltaTable


def salvar_delta(
    df,
    tabela,
    modo="append",
    particionar=False,
    colunas_particao=None,
    overwrite_schema=False,
    usar_merge=False,
    chaves_merge=None,
    colunas_update=None
):
    """
    Salva DataFrame em tabela Delta com suporte a MERGE (upsert).

    Args:
        df: DataFrame Spark para salvar
        tabela: Nome completo da tabela (catalog.schema.table)
        modo: "append", "overwrite", "ignore", "error"
              (ignorado se usar_merge=True)
        particionar: Se True, particiona pelas colunas_particao
        colunas_particao: Lista de colunas para particionamento
        overwrite_schema: Se True, permite alteração de schema
        usar_merge: Se True, usa MERGE (upsert) ao invés de write mode
        chaves_merge: Lista de colunas para condição ON do MERGE
        colunas_update: Dict {coluna_alvo: coluna_fonte} para UPDATE.
                        Se None, atualiza todas as colunas exceto as chaves

    Returns:
        None
    """

    # ============================================================
    # MODO MERGE (upsert)
    # ============================================================

    if usar_merge and chaves_merge:

        if not spark.catalog.tableExists(tabela):

            # Tabela ainda não existe: primeira carga usa overwrite
            # para garantir schema limpo e particionamento correto.
            # NÃO usar append aqui pois a tabela precisa ser criada
            # com as configurações corretas de particionamento.
            log_warning(
                f"Tabela {tabela} não existe. "
                f"Executando OVERWRITE para primeira carga."
            )

            _escrever_delta(
                df=df,
                tabela=tabela,
                modo="overwrite",
                particionar=particionar,
                colunas_particao=colunas_particao,
                overwrite_schema=True
            )
            return

        # --------------------------------------------------------
        # Nome de view único por execução — evita colisão em jobs
        # paralelos que compartilham o mesmo SparkSession
        # --------------------------------------------------------
        view_name = f"staging_merge_{uuid.uuid4().hex[:8]}"

        try:
            df.createOrReplaceTempView(view_name)

            # Condição ON do MERGE
            on_condition = " AND ".join(
                [f"target.{col} = source.{col}"
                 for col in chaves_merge]
            )

            # Colunas para UPDATE — exclui as chaves de merge
            if colunas_update is None:
                df_cols = [f.name for f in df.schema.fields]
                colunas_update = {
                    col: col for col in df_cols
                    if col not in chaves_merge
                }

            # SET clause
            set_clause = ", ".join(
                [f"target.{k} = source.{v}"
                 for k, v in colunas_update.items()]
            )

            # INSERT clause
            insert_cols = ", ".join(
                [f.name for f in df.schema.fields]
            )
            insert_values = ", ".join(
                [f"source.{f.name}" for f in df.schema.fields]
            )

            merge_sql = f"""
                MERGE INTO {tabela} target
                USING {view_name} source
                ON {on_condition}
                WHEN MATCHED THEN
                    UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols})
                    VALUES ({insert_values})
            """

            log_info(f"Executando MERGE em {tabela}")
            spark.sql(merge_sql)
            log_info(f"MERGE concluído em {tabela}")

        finally:
            # Garante limpeza da view mesmo se o MERGE falhar
            spark.sql(f"DROP VIEW IF EXISTS {view_name}")

        return

    # ============================================================
    # MODO PADRÃO (write)
    # ============================================================

    _escrever_delta(
        df=df,
        tabela=tabela,
        modo=modo,
        particionar=particionar,
        colunas_particao=colunas_particao,
        overwrite_schema=overwrite_schema
    )


# ============================================================
# FUNÇÃO AUXILIAR INTERNA — evita duplicação entre os dois
# caminhos de escrita (merge fallback e modo padrão)
# ============================================================

def _escrever_delta(
    df,
    tabela,
    modo,
    particionar,
    colunas_particao,
    overwrite_schema
):
    """
    Escrita direta em Delta sem MERGE.
    Função interna — não chamar diretamente nos notebooks.
    """
    writer = (
        df.write
        .format("delta")
        .mode(modo)
    )

    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")

    if particionar and colunas_particao:
        writer = writer.partitionBy(*colunas_particao)

    writer.saveAsTable(tabela)
