SELECT keyword FROM {{ ref('bridge_card_keywords') }}
UNION
SELECT keyword FROM {{ ref('bridge_print_keywords') }}
