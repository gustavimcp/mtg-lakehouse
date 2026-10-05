-- Substitui set_verbose.csv
SELECT
    set_name,
    min(release_year)                          AS release_year,
    round(avg(no_reminder_length_front), 1)    AS avg_front_face_text_length,
    round(avg(no_reminder_length_both), 1)     AS avg_both_text_length,
    count(*)                                   AS set_number_of_cards
FROM {{ ref('int_texto_cartas') }}
GROUP BY set_name
