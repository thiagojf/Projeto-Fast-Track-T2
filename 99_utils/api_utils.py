import requests
import time


def get_api_data(
    url,
    params=None,
    headers=None,
    max_retries=3,
    retry_delay=2,
    timeout=30
):
    """
    Realiza uma requisição GET para uma API com mecanismo de retry.

    Args:
        url (str): URL da API.
        params (dict, opcional): Parâmetros da query string.
        headers (dict, opcional): Cabeçalhos HTTP.
        max_retries (int): Número máximo de tentativas.
        retry_delay (int): Tempo inicial de espera entre tentativas.
        timeout (int): Tempo máximo de espera da requisição.

    Returns:
        dict ou str:
            Retorna o JSON da resposta ou o texto da resposta.

    Raises:
        requests.exceptions.RequestException:
            Caso todas as tentativas falhem.
    """

    # Loop responsável pelas tentativas de execução
    for tentativa in range(max_retries):

        try:
            # Realiza a chamada HTTP GET
            response = requests.get(
                url=url,
                params=params,
                headers=headers,
                timeout=timeout
            )

            # Lança exceção para códigos HTTP de erro (4xx e 5xx)
            response.raise_for_status()

            # Tenta converter a resposta para JSON
            try:
                return response.json()

            # Caso a resposta não seja JSON, retorna texto puro
            except ValueError:
                return response.text

        # Captura erros relacionados à requisição
        except requests.exceptions.RequestException as e:

            # Verifica se é a última tentativa
            if tentativa == max_retries - 1:
                print(
                    f"Falha após {max_retries} tentativas."
                )
                raise

            # Calcula tempo de espera usando backoff exponencial
            tempo_espera = retry_delay * (2 ** tentativa)

            print(
                f"Tentativa {tentativa + 1} falhou: {e}. "
                f"Nova tentativa em {tempo_espera} segundos..."
            )

            # Aguarda antes de tentar novamente
            time.sleep(tempo_espera)