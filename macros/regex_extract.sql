{% macro regex_extract(string, regex) -%}

{{ adapter.dispatch('regex_extract', 'zendesk') (string, regex) }}

{%- endmacro %}

{% macro default__regex_extract(string, regex) %}

    regexp_extract({{ string }}, {{ regex }} )

{% endmacro %}

{% macro bigquery__regex_extract(string, regex) %}

    regexp_extract({{ string }}, {{ regex }} )

{% endmacro %}

{% macro snowflake__regex_extract(string, regex) %}

    REGEXP_SUBSTR({{ string }}, {{ regex }}, 1, 1, 'e', 1 )

{% endmacro %}

{% macro postgres__regex_extract(string, regex) %}

    (regexp_matches({{ string }}, {{ regex }}))[1]

{% endmacro %}

{% macro redshift__regex_extract(string, regex) %}

    REGEXP_SUBSTR({{ string }}, {{ regex }}, 1, 1, 'e')

{% endmacro %}

{% macro spark__regex_extract(string, regex) %}

    regexp_extract({{ string }}, {{ regex }}, 1)

{% endmacro %}