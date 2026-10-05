-- Substitui unique_keywords_prints.csv
SELECT DISTINCT i.id AS print_id, k.keyword
FROM {{ ref('int_impressoes') }} i, unnest(i.keywords_array) AS k(keyword)
