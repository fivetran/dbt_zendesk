{% macro regex_extract(string, start_or_end) -%}

{{ adapter.dispatch('regex_extract', 'zendesk') (string, start_or_end) }}

{%- endmacro %}

{% macro default__regex_extract(string, start_or_end) %}

REGEXP_EXTRACT({{ string }}, {%- if start_or_end == 'start' %} r'{"([^"]+)"' {% else %} r'":"([^"]+)"}' {% endif -%} )

{% endmacro %}

{% macro bigquery__regex_extract(string, start_or_end) %}

REGEXP_EXTRACT({{ string }}, {%- if start_or_end == 'start' %} r'{"([^"]+)"' {% else %} r'":"([^"]+)"}' {% endif -%} )

{% endmacro %}
