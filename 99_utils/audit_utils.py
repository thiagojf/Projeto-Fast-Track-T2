# Databricks notebook source
from pyspark.sql.functions import *

def adicionar_auditoria(
    df,
    endpoint,
    batch_id,
    pipeline_version
):
    """
    Adiciona colunas de auditoria e controle ao DataFrame.

    Args:
        df (DataFrame):
            DataFrame de entrada.

        endpoint (str):
            Endpoint ou origem dos dados consumidos.

        batch_id (str):
            Identificador único do lote/processamento.

        pipeline_version (str):
            Versão da pipeline responsável pela carga.

    Returns:
        DataFrame:
            DataFrame enriquecido com metadados de auditoria.
    """
    return (
        df
        # Identificador do lote processado
        .withColumn("batch_id", lit(batch_id))
        
        # Data/hora em que o registro foi ingerido na camada atual
        .withColumn("ingested_at", current_timestamp())
        
        # Data/hora da última atualização do registro
        .withColumn("updated_at", current_timestamp())
        
        # Endpoint utilizado para obtenção dos dados
        .withColumn("source_endpoint", lit(endpoint))
        
        # Versão da pipeline responsável pela execução
        .withColumn("pipeline_version", lit(pipeline_version))
        
        # Ano da ingestão (útil para particionamento)
        .withColumn("ano_ingestao", year(current_timestamp()))
        
        # Mês da ingestão (útil para particionamento)
        .withColumn("mes_ingestao", month(current_timestamp()))
    )