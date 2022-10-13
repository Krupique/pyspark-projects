# Spark Streaming Twitter

## Objetivos do Projeto

O projeto `Spark-streaming.ipynb` tem por objetivo, fornecer uma base teórica e prática sobre os conceitos e funcionamento do Spark Streaming. Foi desenvolvido uma demonstração prática de como utilizar as operações de Transformações, Map e Reduce do Spark juntamente com o Streaming.

O projeto `Twitter-streaming.ipynb` tem por objetivo, utilizar a API do Twitter para obter tweets em tempo real de um determinado assunto, aplicar técnicas de MapReduce, transformação de dados com o framework Spark e utilizar o MLlib para classificar tweets como sentimento positivo ou negativo.

O app `tweets-listener.py` é uma aplicação Python que conecta ao Twitter, coleta os dados e então envia via socket para a porta definida.


## Organização dos arquivos e diretórios

* **`Twitter-streaming.ipynb`**: Projeto principal do repositório. Utilizando a API do Twitter para coleta de tweets em tempo real e o Spark para processamento dos dados e classificação de sentimentos dos tweets.
* **`Spark-streaming.ipynb`**: Introdução teórica e prática sobre o Spark Streaming e também sobre a utilização da API do Twitter por meio de conexão via socket.
* **`tweets-listener.py`**: App que coleta os dados do Twitter e envia para o socket.
* **`assets/`**: Diretório de recursos extras.
* **`conf/`**: Diretório contendo o arquivo de configuração config_template.ini. Neste arquivo insira as suas credenciais do Twitter. api key, api key secret, access token, access token secret e bearer token.
* **`data/`**: Diretório que contém o dataset que será utilizado para o treinamento do modelo.


## Análise de Sentimentos de Tweets coletados em tempo real com Spark Streaming e MLlib - Introdução

---
A análise de sentimentos ajuda os analistas de dados de empresas e organizações a avaliar a opinião pública, realizar pesquisas de mercado, monitorar a reputação de marcas e produtos e compreender as experiências dos seus consumidores ou potenciais clientes.

Uma excelente forma de obter dados sobre opiniões, sentimentos e discussões sobre um determinado assunto, é através das redes sociais. Atualmente, o Twitter é a principal rede social para obter informações sobre discussções dos mais variados assuntos e tópicos. A empresa disponibiliza uma API de maneira gratuita para os desenvolvedores que desejam coletar os dados para realizar as suas análises.

A quantidade de tweets gerados por dia pode ultrapassar as centenas de milhares, chegando até em milhões de tweets em um único dia. Para lidar com essa grande quantidade de dados, a utilização de frameworks que sejam capazes de realizar o processamento distribuído em clusters ou em nuvens se faz bastante relevante. A principal ferramenta para realizar esse tipo de tarefa é o Spark. O Spark é uma estrutura de computação em cluster de código aberto, construída em torno da velocidade, facilidade de uso e análise de streaming de dados.

Com a utilização do Spark e a API do Twitter, este projeto se propõe ao desenvolvimento de um modelo de Machine Learning que com base nos dados históricos, seja capaz de criar um modelo para a classificação do sentimento de um texto como sendo positivo ou negativo. A partir disso, novos dados gerados e coletados em tempo real pela API serão submetidos ao modelo e serão classificados de acordo com o respectivo sentimento.


### Sumário do projeto

* 1.) Introdução e definição do problema
  * 1.1) O que é análise de sentimentos?
  * 1.2) Objetivos deste projeto
  * 1.3) Metodologia

* 2.) Parte 1 - Criação do modelo
  * 2.1) Importação das bibliotecas
  * 2.2) Obtendo os dados de treino
  * 2.3) Pré Processamento dos dados
  * 2.4) Modelagem
    * 2.4.1) Obtendo a lista de Stopwords
    * 2.4.2) Treinamento do modelo
    * 2.4.3) Testando o modelo

* 3.) Parte 2 - Coleta de dados do Twitter para Classificação
  * 3.1) Obtendo chaves de autentifação e Definindo regras de pesquisa
  * 3.2) Pré processamento dos Dados
  * 3.3) Coleta e classificação

* 4.) Considerações finais
---
