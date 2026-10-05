-- Preços do snapshot do dia. O Python (mtg_lake.publicar) acrescenta estas linhas
-- ao histórico mensal gold/fct_card_prices/mes=AAAA-MM.parquet.
SELECT
    card_id       AS card_id,
    snapshot_date AS price_date,
    {{ colunas_preco('payload') }},
    current_timestamp AS loaded_at
FROM {{ ref('stg_scryfall__oracle_cards') }}
WHERE {{ filtro_extras('payload') }}
