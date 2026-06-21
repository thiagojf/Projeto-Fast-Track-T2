notebooks = [
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/01_bronze/APICamaraDeputados/bronze_deputados",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/01_bronze/APICamaraDeputados/bronze_frentes",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/01_bronze/APICamaraDeputados/bronze_frentes_membros",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/01_bronze/APICamaraDeputados/bronze_eventos",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/01_bronze/APICamaraDeputados/bronze_eventos_presenca",    
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/01_bronze/APICamaraDeputados/bronze_tipo_evento",    
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/01_bronze/APICamaraDeputados/bronze_despesas",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/01_bronze/APICamaraDeputados/bronze_orgao",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/01_bronze/APICamaraDeputados/bronze_orgaos_detalhe",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/01_bronze/APICamaraDeputados/bronze_votacoes",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/01_bronze/APICamaraDeputados/bronze_votacoes_votos"


]

for notebook in notebooks:

    print(f"Executando notebook: {notebook}")

    dbutils.notebook.run(
        notebook,
        timeout_seconds=3600
    )