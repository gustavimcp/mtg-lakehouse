-- Substitui year_verbose.csv
SELECT
    release_year,
    round(avg(no_reminder_length_front), 1)    AS avg_front_face_text_length,
    round(avg(no_reminder_length_both), 1)     AS avg_both_text_length,
    count(*)                                   AS year_number_of_cards,
    CAST(count_if(oracle_text_both = '') AS BIGINT) AS year_number_of_cards_no_text
FROM {{ ref('int_texto_cartas') }}
GROUP BY release_year
