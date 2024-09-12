{% macro to_json_array(string) -%}

{{ adapter.dispatch('to_json_array', 'zendesk') (string) }}

{%- endmacro %}

{% macro default__to_json_array(string) %}

  json_extract_array({{ string }}, '$')

{% endmacro %}

{% macro redshift__to_json_array(string) %}

  json_parse({{ string }})

{% endmacro %}

{% macro postgres__to_json_array(string) %}

  {{ string }}::jsonb

{% endmacro %}

{% macro snowflake__to_json_array(string) %}

  ARRAY_CONSTRUCT_PARSED({{ string }})

{% endmacro %}

{% macro spark__to_json_array(string) %}

  JSON_ARRAY({{ string }})

{% endmacro %}