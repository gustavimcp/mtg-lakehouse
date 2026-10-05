-- Todas as impressões (em inglês ou no idioma único da carta), JSON bruto do snapshot do dia
SELECT
    json ->> 'id'                              AS card_id,
    json                                       AS payload,
    CAST('{{ var("snapshot_date") }}' AS DATE) AS snapshot_date
FROM read_ndjson_objects({{ bronze('default_cards') }})
