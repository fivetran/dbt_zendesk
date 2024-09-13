{% macro unnest_json_array(string) -%}

{{ adapter.dispatch('unnest_json_array', 'zendesk') (string) }}

{%- endmacro %}

{% macro bigquery__unnest_json_array(string) %}

  unnest(json_extract_array({{ string }}, '$'))

{% endmacro %}

{% macro snowflake__unnest_json_array(string) %}

  lateral flatten(input => parse_json({{ string }}))

{% endmacro %}

{% macro redshift__unnest_json_array(string) %}

  json_array_elements_text('{{ string }}')

{% endmacro %}

{% macro postgres__unnest_json_array(string) %}

  jsonb_array_elements({{ string }}::jsonb)

{% endmacro %}

{% macro spark__unnest_json_array(string) %}

  explode(from_json({{ string }}, 'array<string>'))

{% endmacro %}