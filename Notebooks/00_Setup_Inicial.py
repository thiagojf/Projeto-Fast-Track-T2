# Databricks notebook source
# Célula 1 — Catálogo
spark.sql("CREATE CATALOG IF NOT EXISTS desafio_final_T2")
spark.sql("USE CATALOG desafio_final_T2")
print("✓ Catálogo desafio_final ativo")

# Célula 2 — Schemas
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"✓ Schema {schema} criado")

# Célula 3 — Volumes
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS desafio_final_T2.{schema}.raw
    """)
print(f"✓ Volume desafio_final.{schema}.raw criado")

# Célula 4 — Validação
schemas = spark.sql("SHOW SCHEMAS IN desafio_final_T2/Volumes/desafio_final_t2/bronze/raw_data").collect()
print("Schemas encontrados:")
for s in schemas:
    print(f"  → {s[0]}")
print("\n✓ Setup concluído. Ambiente pronto para o projeto.")

# COMMAND ----------

# ============================================================
# CRIAÇÃO VOLUMES (Unity Catalog)
# ============================================================

# 
spark.sql("USE CATALOG desafio_final_T2")

# Define as camadas e tabelas
schema = ["bronze", "silver", "gold"]
tabelas = ["deputados", "votacoes", "votacoes_votos", "despesas", "frentes","eventos"]

# Cria os schemas (camadas)
for schema in schema:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"✅ Schema criado: {schema}")
    
    # Cria um volume para cada tabela dentro da camada
    for tabela in tabelas:
        spark.sql(f"""
            CREATE VOLUME IF NOT EXISTS {schema}.{tabela}
            COMMENT 'Volume de armazenamento da tabela {tabela} na camada {schema}'
        """)
        print(f"📁 Volume criado: {schema}.{tabela}")