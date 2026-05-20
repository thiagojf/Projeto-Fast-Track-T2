# Databricks notebook source

# ============================================================
# 00_SETUP_INICIAL
# Projeto Final - Engenharia de Dados
# Projeto Analítico Legislativo — Câmara dos Deputados
# ============================================================

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

CATALOG_NAME = "desafio_final_t2"

SCHEMAS = [
    "bronze",
    "silver",
    "gold"
]


# ============================================================
# 2. CRIAÇÃO DO CATÁLOGO
# ============================================================

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}
""")


# ============================================================
# 3. CRIAÇÃO DOS SCHEMAS
# ============================================================

for schema in SCHEMAS:
    spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema}
    """)


# ============================================================
# 4. VALIDAÇÃO
# ============================================================

spark.sql(f"""
SHOW SCHEMAS IN {CATALOG_NAME}
""").show(truncate=False)


# ============================================================
# 5. CONTEXTO DO PROJETO
# ============================================================

spark.sql(f"""
USE CATALOG {CATALOG_NAME}
""")

print("Setup inicial concluído com sucesso.")