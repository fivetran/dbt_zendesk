{% macro sql_slugify(column) %}

{#
    A SQL-side approximation of dbt_utils.slugify() (minus its leading-underscore-on-digit prefix, which only
    matters for producing a valid identifier, not for matching), for normalizing a column value we only know at
    query time (e.g. a custom field title) so it's comparable against a Jinja-side equivalent computed at compile
    time (e.g. a ticket_field_history_columns entry). Not a byte-for-byte match -- it won't collapse repeated
    separators the way the regex-based Python version does -- but close enough for matching purposes. The
    authoritative slugify for the actual column name still happens once, in Jinja.
#}

{%- set ns = namespace(expr = 'lower(' ~ column ~ ')') -%}
{%- for old_char, new_char in [(' ', '_'), ('-', '_'), ('/', ''), (':', ''), (',', ''), ('.', ''), ('(', ''), (')', ''), ('{', ''), ('}', '')] -%}
    {%- set ns.expr = dbt.replace(ns.expr, "'" ~ old_char ~ "'", "'" ~ new_char ~ "'") -%}
{%- endfor -%}
{%- set ns.expr = dbt.replace(ns.expr, "''''", "''") -%}
{{ ns.expr }}

{% endmacro %}
