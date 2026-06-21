notebooks = [
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/02_silver/silver_dim_deputados",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/02_silver/silver_dim_frentes",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/02_silver/silver_bridge_frente_deputado",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/02_silver/silver_dim_tipo_evento",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/02_silver/silver_dim_evento",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/02_silver/silver_dim_orgao",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/02_silver/silver_dim_data",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/02_silver/silver_despesas",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/02_silver/silver_dim_fornecedor",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/02_silver/silver_dim_categoria_despesa",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/02_silver/silver_dim_cpi",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/02_silver/silver_dim_votacao"
]

for notebook in notebooks:

    print(f"Executando notebook: {notebook}")

    dbutils.notebook.run(
        notebook,
        timeout_seconds=0
    )