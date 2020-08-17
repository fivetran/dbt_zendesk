{% macro string_agg(field_to_agg, delimiter) -%}

{{ adapter_macro('zendesk.string_agg', field_to_agg, delimiter) }}

{%- endmacro %}

{% macro default__string_agg(field_to_agg, delimiter) %}
    string_agg({{ field_to_agg }}, {{ delimiter }})

{% endmacro %}

{% macro snowflake__string_agg(field_to_agg, delimiter) %}
    listagg({{ field_to_agg }}, {{ delimiter }})

{% endmacro %}

{% macro redshift__string_agg(field_to_agg, delimiter) %}
    listagg({{ field_to_agg }}, {{ delimiter }})

{% endmacro %}