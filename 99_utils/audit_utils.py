# Databricks notebook source
from pyspark.sql.functions import *

def adicionar_auditoria(
    df,
    endpoint,
    batch_id,
    pipeline_version
):

    return (
        df
        .withColumn("batch_id", lit(batch_id))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
        .withColumn("source_endpoint", lit(endpoint))
        .withColumn("pipeline_version", lit(pipeline_version))
        .withColumn("ano_ingestao", year(current_timestamp()))
        .withColumn("mes_ingestao", month(current_timestamp()))
    )