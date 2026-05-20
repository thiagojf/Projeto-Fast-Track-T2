notebooks = [

    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_1/gold_frentes_membros",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_1/gold_ihh_frentes",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_1/gold_ranking_deputados",
    
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_2/gold_eventos",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_2/gold_vw_eventos_futuros",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_2/gold_densidade_eventos_semana",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_2/gold_taxa_presenca_eventos",  
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_2/gold_comparativo_periodo_eleitoral",

    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modelo_4/gold_fat_despesas",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modelo_4/gold_realtorio_top10_gastos_mensal"

]

for notebook in notebooks:

    print(f"Executando notebook: {notebook}")

    dbutils.notebook.run(
        notebook,
        timeout_seconds=0
    )