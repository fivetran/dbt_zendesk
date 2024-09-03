{% macro count_tokens(column_name) -%}
  {{ return(adapter.dispatch('count_tokens', 'zendesk')(column_name)) }}
{%- endmacro %}

{% macro default__count_tokens(column_name) %}
  {{ dbt.length(column_name) }} / 4
{% endmacro %}