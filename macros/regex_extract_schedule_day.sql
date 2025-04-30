{% macro regex_extract_schedule_day(string, day) -%}

{{ return(adapter.dispatch('regex_extract_schedule_day', 'zendesk') (string, day)) }}

{%- endmacro %}

{% macro default__regex_extract_schedule_day(string, day) %}
    {% set regex = "'.*?" ~ day ~ ".*?({.*?})'" %}
    regexp_extract({{ string }}, {{ regex }} )

{%- endmacro %}

{% macro bigquery__regex_extract_schedule_day(string, day) %}
    {% set regex = "'.*?" ~ day ~ ".*?({.*?})'" %}
    regexp_extract({{ string }}, {{ regex }} )

{%- endmacro %}

{% macro snowflake__regex_extract_schedule_day(string, day) %}
    {% set regex = "'.*?" ~ day ~ ".*?({.*?})'" %}

    REGEXP_SUBSTR({{ string }}, {{ regex }}, 1, 1, 'e', 1 )

{%- endmacro %}

{% macro postgres__regex_extract_schedule_day(string, day) %}
    {% set regex = "'.*?" ~ day ~ ".*?({.*?})'" %}

    (regexp_matches({{ string }}, {{ regex }}))[1]

{%- endmacro %}

{% macro redshift__regex_extract_schedule_day(string, day) %}

    {% set regex = '"' ~ day ~ '"' ~ ':\\\{([^\\\}]*)\\\}' -%}

    '{' || REGEXP_SUBSTR({{ string }}, '{{ regex }}', 1, 1, 'e') || '}'

{%- endmacro %}

{% macro spark__regex_extract_schedule_day(string, day) %}
    {% set regex = "'.*?" ~ day ~ ".*?({.*?})'" | replace("{", "\\\{") | replace("}", "\\\}") %}
    regexp_extract({{ string }}, {{ regex }}, 1)

{%- endmacro %}