{% macro string_cleaner(column) -%}

{%- set cleaned = adapter.dispatch('string_cleaner', 'zendesk') (column) -%}
{#- Replicates dbt_utils.slugify()'s leading-underscore-on-digit prefix, portably (substring/concat work the
    same across every supported adapter), so this always matches the Jinja-side dbt_utils.slugify() output
    exactly -- no need to special-case digits on the Jinja side. #}
{{ return(
    "case when substring(" ~ cleaned ~ ", 1, 1) in ('0','1','2','3','4','5','6','7','8','9') then '_' || " ~ cleaned ~ " else " ~ cleaned ~ " end"
) }}

{%- endmacro %}

{# A SQL approximation of dbt_utils.slugify(), minus its leading-underscore-on-digit prefix (handled above). #}
{% macro default__string_cleaner(column) %}
    regexp_replace(regexp_replace(lower({{ column }}), '[ -]+', '_'), '[^a-z0-9_]+', '')
{%- endmacro %}

{% macro postgres__string_cleaner(column) %}
    regexp_replace(regexp_replace(lower({{ column }}), '[ -]+', '_', 'g'), '[^a-z0-9_]+', '', 'g')
{%- endmacro %}

{#- Prevents fallback to postgres__ dispatch. #}
{% macro redshift__string_cleaner(column) %}
    {{ default__string_cleaner(column) }}
{%- endmacro %}

{% macro duckdb__string_cleaner(column) %}
    regexp_replace(regexp_replace(lower({{ column }}), '[ -]+', '_', 'g'), '[^a-z0-9_]+', '', 'g')
{%- endmacro %}
