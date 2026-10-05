{# Caminho do arquivo bruto de um dataset no snapshot atual (camada bronze) #}
{% macro bronze(dataset) -%}
  {%- if var('snapshot_date') is none -%}
    {{ exceptions.raise_compiler_error("Informe a var snapshot_date (use: python -m mtg_lake transformar)") }}
  {%- endif -%}
  '{{ var("lake") }}/raw/scryfall/{{ dataset }}/dt={{ var("snapshot_date") }}/{{ dataset }}.jsonl.gz'
{%- endmacro %}

{# Remove "extras" que a busca /cards/search do notebook antigo não retornava (tokens, emblemas, art series) #}
{% macro filtro_extras(p='payload') -%}
  ({{ p }} ->> 'layout') NOT IN ('token', 'double_faced_token', 'emblem', 'art_series')
  AND coalesce({{ p }} ->> 'set_type', '') <> 'token'
{%- endmacro %}

{# Lista JSON -> texto "a, b, c" (mesmo formato dos CSVs antigos) #}
{% macro lista_texto(expr) -%}
  array_to_string(CAST({{ expr }} AS VARCHAR[]), ', ')
{%- endmacro %}

{# Formatos em que a carta tem determinado status de legalidade, em ordem alfabética #}
{% macro formatos_com_status(p, status) -%}
  nullif(array_to_string(list_sort(
    [e.key FOR e IN map_entries(json_transform({{ p }} -> 'legalities', '"MAP(VARCHAR, VARCHAR)"')) IF e.value = '{{ status }}']
  ), ', '), '')
{%- endmacro %}

{# Preços vêm como texto na API #}
{% macro colunas_preco(p='payload') -%}
  TRY_CAST({{ p }} ->> '$.prices.usd'        AS DECIMAL(12,2)) AS usd,
  TRY_CAST({{ p }} ->> '$.prices.usd_foil'   AS DECIMAL(12,2)) AS usd_foil,
  TRY_CAST({{ p }} ->> '$.prices.usd_etched' AS DECIMAL(12,2)) AS usd_etched,
  TRY_CAST({{ p }} ->> '$.prices.eur'        AS DECIMAL(12,2)) AS eur,
  TRY_CAST({{ p }} ->> '$.prices.eur_foil'   AS DECIMAL(12,2)) AS eur_foil,
  TRY_CAST({{ p }} ->> '$.prices.tix'        AS DECIMAL(12,2)) AS tix
{%- endmacro %}

{#
  Colunas de carta extraídas do JSON, replicando a lógica do notebook "Script API Scryfall.ipynb".
  Usado tanto para cartas únicas (oracle_cards) quanto para impressões (default_cards).
#}
{% macro colunas_carta(p='payload') -%}
    {{ p }} ->> 'id'                                  AS id,
    {{ p }} ->> 'oracle_id'                           AS oracle_id,
    {{ p }} ->> 'name'                                AS name,
    {{ p }} ->> 'printed_name'                        AS printed_name,
    {{ p }} ->> 'flavor_name'                         AS flavor_name,
    {{ p }} ->> 'lang'                                AS lang,
    TRY_CAST({{ p }} ->> 'released_at' AS DATE)       AS released_at,
    {{ p }} ->> 'uri'                                 AS uri,
    {{ p }} ->> 'scryfall_uri'                        AS scryfall_uri,
    {{ p }} ->> 'layout'                              AS layout,
    {{ p }} ->> 'mana_cost'                           AS mana_cost,
    TRY_CAST({{ p }} ->> 'cmc' AS DOUBLE)             AS cmc,
    {{ p }} ->> 'type_line'                           AS type_line,
    {{ p }} ->> 'oracle_text'                         AS oracle_text,
    {{ p }} ->> 'power'                               AS power,
    {{ p }} ->> 'toughness'                           AS toughness,
    {{ p }} ->> 'loyalty'                             AS loyalty,
    {{ lista_texto(p ~ " -> 'colors'") }}             AS colors,
    {{ lista_texto(p ~ " -> 'color_identity'") }}     AS color_identity,
    {{ lista_texto(p ~ " -> 'keywords'") }}           AS keywords,
    CAST({{ p }} -> 'keywords' AS VARCHAR[])          AS keywords_array,
    {{ lista_texto(p ~ " -> 'produced_mana'") }}      AS produced_mana,
    CAST({{ p }} ->> 'reserved' AS BOOLEAN)           AS reserved,
    CAST({{ p }} ->> 'reprint' AS BOOLEAN)            AS reprint,
    {{ p }} ->> 'set'                                 AS set_code,
    {{ p }} ->> 'set_name'                            AS set_name,
    {{ p }} ->> 'set_type'                            AS set_type,
    {{ p }} ->> 'scryfall_set_uri'                    AS scryfall_set_uri,
    {{ p }} ->> 'collector_number'                    AS collector_number,
    CAST({{ p }} ->> 'digital' AS BOOLEAN)            AS digital,
    {{ p }} ->> 'rarity'                              AS rarity,
    {{ p }} ->> 'flavor_text'                         AS flavor_text,
    {{ p }} ->> 'artist'                              AS artist,
    {{ p }} ->> 'border_color'                        AS border_color,
    {{ p }} ->> 'frame'                               AS frame,
    CAST({{ p }} ->> 'full_art' AS BOOLEAN)           AS full_art,
    CAST({{ p }} ->> 'booster' AS BOOLEAN)            AS booster,
    TRY_CAST({{ p }} ->> 'edhrec_rank' AS INTEGER)    AS edhrec_rank,
    {{ p }} ->> 'security_stamp'                      AS security_stamp,
    -- coluna "games" -> dummies (como no notebook)
    coalesce(list_contains(CAST({{ p }} -> 'games' AS VARCHAR[]), 'arena'), false) AS arena,
    coalesce(list_contains(CAST({{ p }} -> 'games' AS VARCHAR[]), 'mtgo'), false)  AS mtgo,
    coalesce(list_contains(CAST({{ p }} -> 'games' AS VARCHAR[]), 'paper'), false) AS paper,
    {{ formatos_com_status(p, 'legal') }}             AS legal,
    {{ formatos_com_status(p, 'not_legal') }}         AS not_legal,
    {{ formatos_com_status(p, 'banned') }}            AS banned,
    {{ formatos_com_status(p, 'restricted') }}        AS restricted,
    -- imagens: cartas de duas faces não têm image_uris no nível raiz -> usa a frente
    coalesce({{ p }} ->> '$.image_uris.small',  {{ p }} ->> '$.card_faces[0].image_uris.small')  AS image_uri_small,
    coalesce({{ p }} ->> '$.image_uris.normal', {{ p }} ->> '$.card_faces[0].image_uris.normal') AS image_uri_normal,
    coalesce({{ p }} ->> '$.image_uris.large',  {{ p }} ->> '$.card_faces[0].image_uris.large')  AS image_uri_large,
    {{ p }} ->> '$.purchase_uris.tcgplayer'           AS purchase_uri_tcgplayer,
    {{ p }} ->> '$.purchase_uris.cardmarket'          AS purchase_uri_cardmarket,
    -- segunda face (card_back_*), como no notebook
    coalesce(json_array_length({{ p }} -> 'card_faces'), 0) > 1 AS has_card_faces,
    {{ p }} ->> '$.card_faces[1].name'                AS card_back_name,
    {{ p }} ->> '$.card_faces[1].mana_cost'           AS card_back_mana_cost,
    {{ p }} ->> '$.card_faces[1].type_line'           AS card_back_type_line,
    {{ p }} ->> '$.card_faces[1].oracle_text'         AS card_back_oracle_text,
    {{ lista_texto(p ~ " -> '$.card_faces[1].colors'") }} AS card_back_colors,
    {{ p }} ->> '$.card_faces[1].power'               AS card_back_power,
    {{ p }} ->> '$.card_faces[1].toughness'           AS card_back_toughness,
    {{ p }} ->> '$.card_faces[1].loyalty'             AS card_back_loyalty,
    {{ p }} ->> '$.card_faces[1].image_uris.small'    AS card_back_image_uri_small,
    {{ p }} ->> '$.card_faces[1].image_uris.normal'   AS card_back_image_uri_normal,
    {{ p }} ->> '$.card_faces[1].image_uris.large'    AS card_back_image_uri_large,
    -- textos usados nas análises de "verbosidade"
    CASE WHEN coalesce(json_array_length({{ p }} -> 'card_faces'), 0) > 1
         THEN coalesce({{ p }} ->> '$.card_faces[0].oracle_text', '')
         ELSE coalesce({{ p }} ->> 'oracle_text', '') END AS oracle_text_front,
    CASE WHEN coalesce(json_array_length({{ p }} -> 'card_faces'), 0) > 1
         THEN coalesce({{ p }} ->> '$.card_faces[0].oracle_text', '') || coalesce({{ p }} ->> '$.card_faces[1].oracle_text', '')
         ELSE coalesce({{ p }} ->> 'oracle_text', '') END AS oracle_text_both
{%- endmacro %}
