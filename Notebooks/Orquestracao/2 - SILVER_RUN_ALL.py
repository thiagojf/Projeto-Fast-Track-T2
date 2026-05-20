notebooks = [
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_1/silver_dim_deputados",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_1/silver_dim_frentes",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_1/silver_bridge_frente_deputado",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_2/silver_dim_tipo_evento",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_2/silver_dim_evento",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_2/silver_dim_orgao",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_2/silver_dim_data",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modelo_4/silver_despesas",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modelo_4/silver_dim_fornecedor",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modelo_4/silver_dim_categoria_despesa"
]

for notebook in notebooks:

    print(f"Executando notebook: {notebook}")

    dbutils.notebook.run(
        notebook,
        timeout_seconds=0
    )