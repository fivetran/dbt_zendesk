{% macro clean_schedule(column_name) -%}
    replace(replace(replace(replace(cast({{ column_name }} as {{ dbt.type_string() }}), '{', ''), '}', ''), '"', ''), ' ', '')
{%- endmacro %}