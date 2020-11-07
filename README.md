# Stackoverflow_Analysis

Esse repositório contem o código utilizado para minerar os dados do Stackoverflow com o objetivo de otimizar futuras perguntas realizadas no site.

Deve-se adicionar a variável de ambiente "GOOGLE_APPLICATION_CREDENTIALS", com o caminho para sua chave de pesquisa do google cloud (pode ser gerada gratuitamente em https://console.cloud.google.com, crie um projeto, gere a chave e baixe o .json)

Esse projeto conta com o BigQueryHelper, para facilitar a extração de dados, você pode baixa-lo com:

`pip install -e git+https://github.com/SohierDane/BigQuery_Helper#egg=bq_helper`


O seguinte post no Kaggle mostra um pouco de como utilizar essa biblioteca com o stackoverflow: https://www.kaggle.com/paultimothymooney/how-to-query-the-stack-overflow-data

Nesse link tambem é possível utilizar um notebook virtual para fazer as pesquisas, caso queira algo mais dinâmico ou esteja com problemas para preparar o ambiente.

# Entendendo a branch melhor_resposta

Essa branch esta dividida em três partes:

1. A primeira esta no arquivo data_catcher.py. Ela mostra a query utilizada para extrair os dados diretamente do banco de dados.
2. A segunda está no arquivo data_filters.py. Lá os dados são organizados e a variável alvo definida.
3. A terceira ocorre no arquivo neural_network.ipynb. Essa parte é a que sofrerá a maior parte das atualizações, ela contem a criação e teste de modelos de rede neural, assim como algumas operações básicas de preparação de dados.

A partir desse commit (contando com esse) tudo que for modificado será descrito, acompanhe os commits para detalhes. (olhar mensagens de commit e comentários próximos as alterações feitas)