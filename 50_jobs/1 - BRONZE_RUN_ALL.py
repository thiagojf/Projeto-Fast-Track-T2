notebooks = [
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_1/bronze_deputados",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_1/bronze_frentes",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_1/bronze_frentes_membros",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_2/bronze_eventos",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_2/bronze_eventos_presenca",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modulo_2/bronze_tipo_evento",
    "/Workspace/Users/thiagofaria87@escoladotrabalhador40.com.br/Desafio_Final_Compass_V2/Modelo_4/bronze_despesas"


]

for notebook in notebooks:

    print(f"Executando notebook: {notebook}")

    dbutils.notebook.run(
        notebook,
        timeout_seconds=0
    )