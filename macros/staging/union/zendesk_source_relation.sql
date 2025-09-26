{% macro apply_source_relation() -%}

{{ adapter.dispatch('apply_source_relation', 'zendesk') () }}

{%- endmacro %}

{% macro default__apply_source_relation() -%}

{% if var('zendesk_sources', []) != [] %}
, _dbt_source_relation as source_relation
{% else %}
, '{{ var("zendesk_database", target.database) }}' || '.'|| '{{ var("zendesk_schema", "zendesk") }}' as source_relation
{% endif %} 

{%- endmacro %}