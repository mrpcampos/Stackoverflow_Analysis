import bq_helper
from bq_helper import BigQueryHelper
stackOverflow = bq_helper.BigQueryHelper(active_project="bigquery-public-data", dataset_name="stackoverflow")
bq_assistant = BigQueryHelper("bigquery-public-data", "stackoverflow")

tabelas = bq_assistant.list_tables()
esquemas_tabelas = {}

query = """
SELECT Year, Tag, Total, Percent_Questions_with_Answers
FROM (SELECT EXTRACT(YEAR FROM a.creation_date) as Year, t.tag_name as Tag, COUNT(1) as Total, ROUND(100 * SUM(IF(a.answer_count > 0, 1, 0)) / COUNT(*), 1) AS Percent_Questions_with_Answers
FROM `bigquery-public-data.stackoverflow.posts_questions` a right JOIN `bigquery-public-data.stackoverflow.tags` t ON t.tag_name in UNNEST(SPLIT(a.tags,'|'))
GROUP BY Year, Tag
HAVING
  Year > 2019 AND Year < 2021
ORDER BY
  Total DESC
LIMIT 20)
ORDER BY Percent_Questions_with_Answers DESC
"""
response = stackOverflow.query_to_pandas_safe(query, max_gb_scanned=20)
response.head(20)

query1 = "SELECT tag_name as Assunto, count as Num_perguntas FROM `bigquery-public-data.stackoverflow.tags` order BY count DESC"
response1 = stackOverflow.query_to_pandas_safe(query1, max_gb_scanned=20)
response1.head(20)

query2 = """SELECT
  Day_of_Week,
  COUNT(1) AS Num_Questions,
  SUM(answered_in_1h) AS Num_Answered_in_1H,
  ROUND(100 * SUM(answered_in_1h) / COUNT(1),1) AS Percent_Answered_in_1H
FROM
(
  SELECT
    q.id AS question_id,
    EXTRACT(DAYOFWEEK FROM q.creation_date) AS day_of_week,
    MAX(IF(a.parent_id IS NOT NULL AND
           (UNIX_SECONDS(a.creation_date)-UNIX_SECONDS(q.creation_date))/(60*60) <= 1, 1, 0)) AS answered_in_1h
  FROM
    `bigquery-public-data.stackoverflow.posts_questions` q
  LEFT JOIN
    `bigquery-public-data.stackoverflow.posts_answers` a
  ON q.id = a.parent_id
  WHERE EXTRACT(YEAR FROM a.creation_date) = 2016
    AND EXTRACT(YEAR FROM q.creation_date) = 2016
  GROUP BY question_id, day_of_week
)
GROUP BY
  Day_of_Week
ORDER BY
  Day_of_Week;
        """
response2 = stackOverflow.query_to_pandas_safe(query2, max_gb_scanned=10)
response2.head(10)

query3 = """SELECT
  EXTRACT(YEAR FROM creation_date) AS Year,
  COUNT(*) AS Number_of_Questions,
  ROUND(100 * SUM(IF(answer_count > 0, 1, 0)) / COUNT(*), 1) AS Percent_Questions_with_Answers
FROM
  `bigquery-public-data.stackoverflow.posts_questions`
GROUP BY
  Year
HAVING
  Year > 2008 AND Year < 2016
ORDER BY
  Year;
        """
response3 = stackOverflow.query_to_pandas_safe(query3)
response3.head(10)

for t in tabelas:
	esquemas_tabelas[t] = bq_assistant.table_schema(t)

obj = esquemas_tabelas["tags"].head()
print(obj)
print(obj.values)
