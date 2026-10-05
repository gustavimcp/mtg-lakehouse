-- Substitui mtg_sets.csv e set_icons.csv
SELECT
    payload ->> 'id'                                AS set_id,
    payload ->> 'code'                              AS set_code,
    payload ->> 'name'                              AS set_name,
    TRY_CAST(payload ->> 'released_at' AS DATE)     AS released_at,
    payload ->> 'set_type'                          AS set_type,
    TRY_CAST(payload ->> 'card_count' AS INTEGER)   AS card_count,
    CAST(payload ->> 'digital' AS BOOLEAN)          AS digital,
    payload ->> 'parent_set_code'                   AS parent_set_code,
    payload ->> 'block'                             AS block,
    payload ->> 'icon_svg_uri'                      AS icon_svg_uri,
    payload ->> 'scryfall_uri'                      AS scryfall_uri
FROM {{ ref('stg_scryfall__sets') }}
