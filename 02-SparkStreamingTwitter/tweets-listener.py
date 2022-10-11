import requests
import configparser
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
import socket
import json
import time

# Nestas configs estão as chaves da api do Twiterr
# Você vai encontrar um arquivo chamado config_template.ini no diretório conf. renomeie para config.ini e insira suas credenciais do Twitter
config = configparser.ConfigParser()
config.read('conf/config.ini')

# Obtendo as credenciais
api_key = config['twitter']['api_key']
api_key_secret = config['twitter']['api_key_secret']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']

# O bearer token da erro no arquivo config. Portanto, eu inseri direto aqui.
bearer_token = r'AAAAAAAAAAAAAAAAAAAAAHY5hgEAAAAA2FSk9Qi3r1OKDdlXRzhkzox9InI%3DR3U3eL0OI5G8ka9GO1OlXtrZnvfuktbrEJozzVwPz1LssHYWcG'

# Obtendo acesso e conectando-se à API do Twitter usando credenciais
client = tweepy.Client(bearer_token, api_key, api_key_secret, access_token, access_token_secret)

# Realizando autenticação
auth = tweepy.OAuth1UserHandler(api_key, api_key_secret, access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

# Termos de busca 
search_terms = ["russia", 'war has:vidoes', 'putin']

lst_text = []
# Criação da class Streaming
# Veja que a classe herda da classe Tweepy.StreamingClient
class MyStream(tweepy.StreamingClient):

    # vamos setar o socket específico.
    def set_socket(self, csocket):
        self.client_socket = csocket

    # Obtendo os dados coletados e enviando via socket para a porta selecionada.
    def on_data(self, tweet):
        try:
            tweet = json.loads(tweet.decode('utf-8'))
            text = tweet['data']['text']

            print(text)
            self.client_socket.send( text.encode('utf-8') )

            return True

        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True


# Este é o método que iremos utilizar para instanciar a classe MyStream, coletar so tweets e enviar para a porta. 
def sendData(c_socket):
    # Instanciando a classe
    stream = MyStream(bearer_token=bearer_token)

    # Setando o socket na classe.
    stream.set_socket(c_socket)

    # Adicionando as novas regras
    for term in search_terms:
        stream.add_rules(tweepy.StreamRule(term))

    # Iniciando o streaming de dados
    stream.filter(tweet_fields=["text"])


# Autenticação utilizando o Bearer Token
def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


# Obtendo as regras atuais
def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print('Get rules')
    print(json.dumps(response.json()))
    return response.json()

# Deletando todas as regras
def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print('Delete all rules')
    print(json.dumps(response.json()))
    return response.json()

# Função main da aplicação
if __name__ == "__main__":
    # Criação do socket
    s = socket.socket()

    # Obtendo IP da máquina local
    host = "127.0.0.1"
    # Reservando a porta 5554 para rodar o serviço
    port = 5554         
    # Ligando o socket      
    s.bind((host, port))

    print("Listening on port: %s" % str(port))

    # Agora aguarde a conexão do cliente.
    s.listen(5)
    # Estabelece conexão com o cliente.        
    c, addr = s.accept()

    # Executando as funções obter e deletar regras atuais
    rules = get_rules()
    delete = delete_all_rules(rules)

    print("Received request from: " + str(addr))

    # Iniciando serviço de coleta de dados
    sendData(c)