{% macro regex_extract(string, start_or_end) -%}

{{ adapter.dispatch('regex_extract', 'zendesk') (string, start_or_end) }}

{%- endmacro %}

{% macro default__regex_extract(string, start_or_end) %}

REGEXP_EXTRACT({{ string }}, {%- if start_or_end == 'start' %} r'{"([^"]+)"' {% else %} r'":"([^"]+)"}' {% endif -%} )

{% endmacro %}

{% macro bigquery__regex_extract(string, start_or_end) %}

REGEXP_EXTRACT({{ string }}, {%- if start_or_end == 'start' %} r'{"([^"]+)"' {% else %} r'":"([^"]+)"}' {% endif -%} )

{% endmacro %}

{% macro snowflake__regex_extract(string, start_or_end) %}

REGEXP_SUBSTR({{ string }}, {%- if start_or_end == 'start' %} '"([^"]+)"' {% else %} '":"([^"]+)"' {% endif -%}, 1, 1, 'e', 1 )

{% endmacro %}

{% macro postgres__regex_extract(string, start_or_end) %}

(regexp_matches({{ string }}, {%- if start_or_end == 'start' %} '"([^"]+)":' {% else %} '": "([^"]+)' {% endif -%} ))[1]

{% endmacro %}

{% macro redshift__regex_extract(string, start_or_end) %}

REGEXP_SUBSTR({{ string }}, {%- if start_or_end == 'start' %} '"([^"]+)"' {% else %} '": "([^"]+)"' {% endif -%}, 1, 1, 'e')

{% endmacro %}

{% macro spark__regex_extract(string, start_or_end) %}

regexp_extract({{ string }}, {%- if start_or_end == 'start' %} '"([^"]+)":' {% else %} '": "([^"]+)"' {% endif -%}, 1)

{% endmacro %}