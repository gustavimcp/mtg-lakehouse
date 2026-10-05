-- Base das análises de "verbosidade": impressões de sets do tipo "expansion" já lançadas,
-- sem repetir (nome, set, texto). O texto de lembrete entre parênteses é removido.
SELECT
    name,
    set_name,
    year(released_at) AS release_year,
    oracle_text_both,
    length(regexp_replace(oracle_text_front, '[(][^)]*[)]', '', 'g')) AS no_reminder_length_front,
    length(regexp_replace(oracle_text_both,  '[(][^)]*[)]', '', 'g')) AS no_reminder_length_both
FROM {{ ref('dim_prints') }}
WHERE set_type = 'expansion'
  AND released_at <= current_date
QUALIFY row_number() OVER (PARTITION BY name, set_name, oracle_text ORDER BY id) = 1
