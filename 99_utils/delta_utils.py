# Databricks notebook source
# Databricks notebook source

def salvar_delta(
    df,
    tabela,
    modo="append",
    particionar=False,
    colunas_particao=None,
    overwrite_schema=False
):

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