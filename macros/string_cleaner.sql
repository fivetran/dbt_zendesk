{% macro string_cleaner(column) -%}

{{ return(adapter.dispatch('string_cleaner', 'zendesk') (column)) }}

{%- endmacro %}

{# A SQL approximation of dbt_utils.slugify() #}
{% macro default__string_cleaner(column) %}
    regexp_replace(regexp_replace(lower({{ column }}), '[ -]+', '_'), '[^a-z0-9_]+', '')
{%- endmacro %}

{% macro postgres__string_cleaner(column) %}
    regexp_replace(regexp_replace(lower({{ column }}), '[ -]+', '_', 'g'), '[^a-z0-9_]+', '', 'g')
{%- endmacro %}

{#- Prevents fallback to postgres__ dispatch. #}
{% macro redshift__string_cleaner(column) %}
    {{ default__string_cleaner(column) }}
{%- endmacro %}

{% macro duckdb__string_cleaner(column) %}
    regexp_replace(regexp_replace(lower({{ column }}), '[ -]+', '_', 'g'), '[^a-z0-9_]+', '', 'g')
{%- endmacro %}
