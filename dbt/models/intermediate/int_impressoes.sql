-- Impressões já em colunas (sem tokens, emblemas e art series)
SELECT {{ colunas_carta('payload') }}
FROM {{ ref('stg_scryfall__default_cards') }}
WHERE {{ filtro_extras('payload') }}
