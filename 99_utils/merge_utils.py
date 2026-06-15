# Databricks notebook source
from delta.tables import DeltaTable

def executar_merge(
    df,
    tabela_destino,
    condicao_merge
):
    # AprovadosVerifica se a tabela existe
    if spark.catalog.tableExists(tabela_destino):

        (
            DeltaTable
            .forName(spark, tabela_destino)
            .alias("dest")
            .merge(
                df.alias("orig"),
                condicao_merge
            )
            .whenMatchedUpdateAll()   # Atualiza todas as colunas automaticamente
            .whenNotMatchedInsertAll()  # Insere todas as colunas automaticamente
            .execute()
        )

    else:

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(tabela_destino)
        )