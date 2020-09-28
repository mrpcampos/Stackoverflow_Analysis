# Stackoverflow_Analysis

Esse repositório contem o código utilizado para minerar os dados do Stackoverflow com o objetivo de otimizar futuras perguntas realizadas no site.

Deve-se adicionar a variável de ambiente "GOOGLE_APPLICATION_CREDENTIALS", com o caminho para sua chave de pesquisa do google cloud (pode ser gerada gratuitamente em https://console.cloud.google.com, crie um projeto, gere a chave e baixe o .json)

Esse projeto conta com o BigQueryHelper, para facilitar a extração de dados, você pode baixa-lo com:
`pip install -e git+https://github.com/SohierDane/BigQuery_Helper#egg=bq_helper`