{% macro sql_slugify(column) -%}

{{ return(adapter.dispatch('sql_slugify', 'zendesk') (column)) }}

{%- endmacro %}

{#
    A SQL-side equivalent of dbt_utils.slugify() (minus its leading-underscore-on-digit prefix, which only
    matters for producing a valid identifier, not for matching), for normalizing a column value we only know at
    query time (e.g. a custom field title) so it's comparable against a Jinja-side equivalent computed at compile
    time (e.g. a ticket_field_history_columns entry). The authoritative slugify for the actual column name still
    happens once, in Jinja.
#}

{% macro default__sql_slugify(column) %}
    regexp_replace(regexp_replace(lower({{ column }}), '[ -]+', '_'), '[^a-z0-9_]+', '')
{%- endmacro %}

{% macro postgres__sql_slugify(column) %}
    regexp_replace(regexp_replace(lower({{ column }}), '[ -]+', '_', 'g'), '[^a-z0-9_]+', '', 'g')
{%- endmacro %}

{#- Redshift's adapter dispatch falls back to postgres__ when no redshift__ variant exists, but Redshift's
    regexp_replace (unlike Postgres') already replaces every match by default and rejects a 'g' parameter
    outright -- so this explicitly redirects back to the flag-free default__ instead of inheriting postgres__. #}
{% macro redshift__sql_slugify(column) %}
    {{ default__sql_slugify(column) }}
{%- endmacro %}

{% macro duckdb__sql_slugify(column) %}
    regexp_replace(regexp_replace(lower({{ column }}), '[ -]+', '_', 'g'), '[^a-z0-9_]+', '', 'g')
{%- endmacro %}
