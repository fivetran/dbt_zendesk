{% macro regex_extract_support_role_change(string) -%}

{{ return(adapter.dispatch('regex_extract_support_role_change', 'zendesk') (string)) }}

{%- endmacro %}

{% macro default__regex_extract_support_role_change(string) %}
    regexp_extract({{ string }}, 'support role changed from (.*)', 1)
{%- endmacro %}

{% macro bigquery__regex_extract_support_role_change(string) %}
    regexp_extract({{ string }}, r'support role changed from (.*)', 1)
{%- endmacro %}

{% macro snowflake__regex_extract_support_role_change(string) %}
    regexp_substr({{ string }}, 'support role changed from (.*)', 1, 1, NULL, 1)
{%- endmacro %}

{% macro postgres__regex_extract_support_role_change(string) %}
    (regexp_matches({{ string }}, 'support role changed from (.*)'))[1]
{%- endmacro %}

{% macro redshift__regex_extract_support_role_change(string) %}
    REGEXP_SUBSTR({{ string }}, 'support role changed from (.*)', 1, 1, 'e', 1)
{%- endmacro %}

{% macro spark__regex_extract_support_role_change(string) %}
    regexp_extract({{ string }}, 'support role changed from (.*)', 1)
{%- endmacro %}