{% macro fivetran_date_part(datepart, date) -%}
    {{ adapter.dispatch('fivetran_date_part', 'zendesk') (datepart, date) }}
{%- endmacro %}

{% macro default__fivetran_date_part(datepart, date) -%}
    date_part('{{ datepart }}', {{  date }})
{%- endmacro %}

{% macro bigquery__fivetran_date_part(datepart, date) -%}
    extract({{ datepart }} from {{ date }})
{%- endmacro %}