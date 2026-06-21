notebooks = [

    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_frentes_membros",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_ihh_frentes",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_ranking_deputados",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_eventos",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_vw_eventos_futuros",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_densidade_eventos_semana",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_taxa_presenca_eventos",  
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_comparativo_periodo_eleitoral",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_fat_despesas",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_realtorio_top10_gastos_mensal",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_engajamento_deputado",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_engajamento_partido",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_cpis",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_frentes_alinhamento",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/GIT_Projeto-Fast-Track-T2/01_src/03_gold/gold_frentes_votacoes"

]

for notebook in notebooks:

    print(f"Executando notebook: {notebook}")

    dbutils.notebook.run(
        notebook,
        timeout_seconds=0
    )