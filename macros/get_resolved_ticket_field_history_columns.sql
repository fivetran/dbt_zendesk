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
    {% set results_list = dbt_utils.get_column_values(ref('stg_zendesk__ticket_field_history'), 'field_name', default=[]) %}

    {% set custom_field_names = {} %}
    {% if var('using_ticket_custom_field', True) %}
        {% set custom_field_results = dbt_utils.get_query_results_as_dict("select ticket_custom_field_id, coalesce(title, raw_title) as resolved_name from " ~ ref('stg_zendesk__ticket_custom_field')) %}
        {% if custom_field_results %}
            {% for id, name in zip(custom_field_results['ticket_custom_field_id'], custom_field_results['resolved_name']) %}
                {% if name %}
                    {% do custom_field_names.update({id | string: name}) %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}

    {% set columns_to_pivot = results_list | select("in", var('ticket_field_history_columns')) | list %}

    -- First pass: resolve each column to its (not yet deduplicated) slugified name.
    {% set slugified_names = {} %}
    {% for col in columns_to_pivot %}
        {% set resolved_name = custom_field_names.get(col | string, col) %}
        {% do slugified_names.update({col: dbt_utils.slugify(resolved_name)}) %}
    {% endfor %}

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
