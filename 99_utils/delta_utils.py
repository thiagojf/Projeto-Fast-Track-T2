# Databricks notebook source
from pyspark.sql import functions as F

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
    Salva dataframe em tabela Delta com suporte a MERGE (upsert).
    
    Args:
        df: DataFrame Spark para salvar
        tabela: Nome completo da tabela (schema.table)
        modo: "append", "overwrite", "ignore", "error" (ignorado se usar_merge=True)
        particionar: Se True, particiona pela colunas_particao
        colunas_particao: Lista de colunas para particionamento
        overwrite_schema: Se True, permite alteração de schema
        usar_merge: Se True, usa MERGE (upsert) ao invés de write mode
        chaves_merge: Lista de colunas para ON condition do MERGE
        colunas_update: Dicionário {coluna_alvo: coluna_fonte} para UPDATE
                       Se None, atualiza todas as colunas menos as chaves
    
    Returns:
        None
    """
    
    # Modo MERGE (upsert)
    if usar_merge and chaves_merge:
        
        # Validação da tabela
        if not spark.catalog.tableExists(tabela):
            log_warning(f"Tabela {tabela} não existe. Usando 'append' para criar.")
            usar_merge = False
        else:
            # Criar view temporária para staging
            df.createOrReplaceTempView("staging_merge")
            
            # Construir cláusula ON do MERGE
            on_condition = " AND ".join(
                [f"target.{col} = source.{col}" for col in chaves_merge]
            )
            
            # Definir colunas para UPDATE
            if colunas_update is None:
                # Atualizar todas as colunas exceto as chaves
                df_schema_cols = [f.name for f in df.schema.fields]
                colunas_update = {
                    col: col for col in df_schema_cols 
                    if col not in chaves_merge
                }
            
            # Construir SET clause para UPDATE
            set_clause = ",".join(
                [f"target.{k} = source.{v}" for k, v in colunas_update.items()]
            )
            
            # Construir INSERT clause
            insert_cols = ",".join(
                [f.name for f in df.schema.fields]
            )
            insert_values = ",".join(
                [f"source.{f.name}" for f in df.schema.fields]
            )
            
            # Executar MERGE
            merge_sql = f"""
            MERGE INTO {tabela} target
            USING staging_merge source
            ON {on_condition}
            WHEN MATCHED THEN
              UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols})
              VALUES ({insert_values})
            """
            
            log_info(f"Executando MERGE em {tabela}")
            spark.sql(merge_sql)
            
            # Limpar view temporária
            spark.sql("DROP VIEW IF EXISTS staging_merge")
            
            return
    
    # Modo padrão (write)
    writer = (
        df.write
        .format("delta")
        .mode(modo)
    )

    if overwrite_schema:
        writer = writer.option(
            "overwriteSchema",
            "true"
        )

    if particionar and colunas_particao:
        writer = writer.partitionBy(
            *colunas_particao
        )

    writer.saveAsTable(tabela)