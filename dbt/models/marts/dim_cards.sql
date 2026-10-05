-- Substitui mtg_dimensao.csv (uma linha por carta única).
-- Listas (keywords_array) ficam de fora: o Power BI não lê colunas de lista em Parquet.
SELECT * EXCLUDE (keywords_array)
FROM {{ ref('int_cartas') }}
