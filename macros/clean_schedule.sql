{% macro clean_schedule(column_name) -%}
    {{ return(adapter.dispatch('clean_schedule', 'zendesk')(column_name)) }}
{%- endmacro %}

{% macro default__clean_schedule(column_name) -%}
    replace(replace(replace(replace({{ column_name }}, '{', ''), '}', ''), '"', ''), ' ', '')
{%- endmacro %}