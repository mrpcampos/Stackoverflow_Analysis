from bq_helper import BigQueryHelper

stackOverflow = BigQueryHelper(active_project="bigquery-public-data", dataset_name="stackoverflow")

query = """
SELECT q.id as id_pergunta, q.accepted_answer_id as target,
a.id as id_resposta, a.score as pontos_resposta, TIMESTAMP_DIFF(a.creation_date, q.creation_date, minute) as tempo_resposta, u.reputation as user_reputation,
(u.website_url is not null) as tem_site_proprio, count(4 - b.class) as maestria_tags

from `bigquery-public-data.stackoverflow.posts_questions` q
inner join `bigquery-public-data.stackoverflow.posts_answers` a on a.parent_id = q.id AND a.owner_user_id IS NOT NULL
inner join `bigquery-public-data.stackoverflow.users` u on a.owner_user_id = u.id
left join `bigquery-public-data.stackoverflow.badges` b ON b.tag_based = TRUE AND b.name in UNNEST(SPLIT(q.tags,'|')) AND b.user_id = a.owner_user_id 

WHERE q.answer_count > 1 AND q.accepted_answer_id is not NULL
AND q.creation_date > '2019-01-01 00:00:00'
AND q.creation_date < '2020-01-01 00:00:00'

group by id_pergunta, target, id_resposta, pontos_resposta, tempo_resposta, user_reputation, tem_site_proprio
order by id_pergunta, pontos_resposta desc
"""

response = stackOverflow.query_to_pandas_safe(query, max_gb_scanned=50)
print(response.head(20))

print("Salvando para csv...")
response.to_csv('perguntas_2019.csv')
print("Csv salvo!")

