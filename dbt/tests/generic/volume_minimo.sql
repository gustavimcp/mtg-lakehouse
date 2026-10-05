{# Falha se o modelo tiver menos linhas que o mínimo (ex.: arquivo da Scryfall truncado) #}
{% test volume_minimo(model, minimo) %}
SELECT COUNT(*) AS linhas
FROM {{ model }}
HAVING COUNT(*) < {{ minimo }}
{% endtest %}
