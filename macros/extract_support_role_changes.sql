{% macro extract_support_role_changes(field) -%}
{{ return(adapter.dispatch('extract_support_role_changes', 'zendesk') (field)) }}
{%- endmacro %}

{% macro default__extract_support_role_changes(field) %}
    {{ dbt.split_part(
        dbt.split_part(field, "'support role changed from '", 2),
        "'\\n'", 1)
    }}
{%- endmacro %}

{% macro postgres__extract_support_role_changes(field) %}
    {{ dbt.split_part(
        dbt.split_part(field, "'support role changed from '", 2),
        "'\n'", 1)
    }}
{%- endmacro %}

{% macro spark__extract_support_role_changes(field) %}
    regexp_extract({{ field }}, 'support role changed from (.*)', 1)
{%- endmacro %}