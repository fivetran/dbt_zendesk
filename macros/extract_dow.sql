{% macro extract_dow(date_or_time) -%}
  {{ return(adapter.dispatch('extract_dow', 'zendesk')(date_or_time)) }}
{%- endmacro %}

-- Snowflake and Postgres use DOW where Sunday = 0
{% macro default__extract_dow(date_or_time) %}
  extract(dow from {{ date_or_time }})
{% endmacro %}

-- BigQuery and Databricks use DAYOFWEEK where Sunday = 1, so subtract 1 to make Sunday = 0
{% macro bigquery__extract_dow(date_or_time) %}
  (extract(dayofweek from {{ date_or_time }}) - 1)
{% endmacro %}

{% macro spark__extract_dow(date_or_time) %}
  (extract(dayofweek from {{ date_or_time }}) - 1)
{% endmacro %}