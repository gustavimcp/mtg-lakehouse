{# Usa o nome do schema exatamente como configurado (gold, staging...), sem prefixo "main_" #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
