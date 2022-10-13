# Spark Streaming Twitter

## Análise de Sentimentos de Tweets coletados em tempo real com Spark Streaming e MLlib

---
A análise de sentimentos ajuda os analistas de dados de empresas e organizações a avaliar a opinião pública, realizar pesquisas de mercado, monitorar a reputação de marcas e produtos e compreender as experiências dos seus consumidores ou potenciais clientes.

Uma excelente forma de obter dados sobre opiniões, sentimentos e discussões sobre um determinado assunto, é através das redes sociais. Atualmente, o Twitter é a principal rede social para obter informações sobre discussções dos mais variados assuntos e tópicos. A empresa disponibiliza uma API de maneira gratuita para os desenvolvedores que desejam coletar os dados para realizar as suas análises.

A quantidade de tweets gerados por dia pode ultrapassar as centenas de milhares, chegando até em milhões de tweets em um único dia. Para lidar com essa grande quantidade de dados, a utilização de frameworks que sejam capazes de realizar o processamento distribuído em clusters ou em nuvens se faz bastante relevante. A principal ferramenta para realizar esse tipo de tarefa é o Spark. O Spark é uma estrutura de computação em cluster de código aberto, construída em torno da velocidade, facilidade de uso e análise de streaming de dados.

Com a utilização do Spark e a API do Twitter, este projeto se propõe ao desenvolvimento de um modelo de Machine Learning que com base nos dados históricos, seja capaz de criar um modelo para a classificação do sentimento de um texto como sendo positivo ou negativo. A partir disso, novos dados gerados e coletados em tempo real pela API serão submetidos ao modelo e serão classificados de acordo com o respectivo sentimento.

---