# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW desafio_final_t2.gold.vw_eventos_futuros AS
# MAGIC SELECT *
# MAGIC FROM desafio_final_t2.gold.eventos
# MAGIC WHERE data_evento > current_date()