{% macro get_resolved_ticket_field_history_columns() %}

{#
    Returns a dict of {raw ticket_field_history_columns entry: final column alias}, covering only the entries
    that are both requested via ticket_field_history_columns and actually present in ticket_field_history.

    Custom (numeric ID) entries are resolved to their ticket_custom_field title/raw_title and slugified. Standard
    fields (status, priority, etc.) fall back to their raw name, also slugified. If two different entries resolve
    to the same slugified name (e.g. two custom fields sharing a title), the raw id/name is appended to the
    losing entries to keep column names unique.
#}

{% set resolved_columns = {} %}

{% if execute and flags.WHICH in ('run', 'build') -%}
    -- int_zendesk__field_history_column_names already resolved each tracked field to its human-readable name
    -- (or its own raw name for standard fields) as an actual table, so this is a cheap read, not a re-join.
    {% set resolved_results = dbt_utils.get_query_results_as_dict("select field_name, resolved_name from " ~ ref('int_zendesk__field_history_column_names')) %}

    -- First pass: slugify each column's resolved name (not yet deduplicated).
    {% set slugified_names = {} %}
    {% if resolved_results %}
        {% for col, resolved_name in zip(resolved_results['field_name'], resolved_results['resolved_name']) %}
            {% do slugified_names.update({col: dbt_utils.slugify(resolved_name)}) %}
        {% endfor %}
    {% endif %}

    -- Count how many columns land on the same slugified name so we know which ones need disambiguating.
    {% set name_counts = {} %}
    {% for col, name in slugified_names.items() %}
        {% do name_counts.update({name: name_counts.get(name, 0) + 1}) %}
    {% endfor %}

    -- Second pass: append the raw id/name to any column whose slugified name collides with another's.
    {% for col, name in slugified_names.items() %}
        {% if name_counts[name] > 1 %}
            {% set suffix = dbt_utils.slugify(col | string) %}
            {% set suffix = suffix[1:] if suffix.startswith('_') else suffix %}
            {% do resolved_columns.update({col: name ~ '_' ~ suffix}) %}
        {% else %}
            {% do resolved_columns.update({col: name}) %}
        {% endif %}
    {% endfor %}
{% endif -%}

{{ return(resolved_columns) }}

{% endmacro %}
