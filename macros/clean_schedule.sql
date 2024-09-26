{% macro clean_schedule(column_name) -%}
    replace(replace(replace(replace({{ column_name }}, '{', ''), '}', ''), '"', ''), ' ', '')
{%- endmacro %}