-- Substitui unique_keywords.csv. Modelo no Power BI: dim_keywords 1-* bridge *-1 dim_cards
SELECT DISTINCT i.id AS card_id, k.keyword
FROM {{ ref('int_cartas') }} i, unnest(i.keywords_array) AS k(keyword)
