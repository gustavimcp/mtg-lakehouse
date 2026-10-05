-- Substitui keyword_pairs.csv (compatibilidade com o modelo antigo do MTG_2)
SELECT DISTINCT b.keyword, p.keywords AS matched_string
FROM {{ ref('bridge_print_keywords') }} b
JOIN {{ ref('dim_prints') }} p ON p.id = b.print_id
