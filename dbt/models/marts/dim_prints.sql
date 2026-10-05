-- Substitui mtg_dimensao_prints.csv (uma linha por impressão).
-- Mesma regra do notebook: se o nome da impressão não existe na dimensão de
-- cartas únicas, usa apenas a parte antes de " //".
WITH nomes_padrao AS (
    SELECT DISTINCT name FROM {{ ref('int_cartas') }}
)
SELECT
    i.* EXCLUDE (keywords_array)
        REPLACE (CASE WHEN n.name IS NULL THEN split_part(i.name, ' //', 1) ELSE i.name END AS name)
FROM {{ ref('int_impressoes') }} i
LEFT JOIN nomes_padrao n ON n.name = i.name
