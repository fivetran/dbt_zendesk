{% macro resolve_ticket_field_history_columns() %}

{#
    Returns a dict of {field_name: column alias} covering every entry in the `ticket_field_history_columns`
    variable that actually appears in the ticket field history data.

    Custom fields may be referenced by either their numeric ID or their title, since Zendesk stores custom field
    history under the ID but customers only ever see the title. Either way the column is named after the title.
    Standard fields (status, priority, etc.) have no custom field record and use their own name.

    Matching compares slugified values, so an entry doesn't have to match the stored title's exact spacing or
    punctuation. If two fields resolve to the same alias, the raw field_name is appended to keep them unique.

    Returns an empty dict outside of a run/build (e.g. during `dbt compile`), so callers can always reference the
    result without guarding it themselves.
#}

{% set resolved_columns = {} %}

{% if execute and flags.WHICH in ('run', 'build') %}

    {# Sorted so the pivot emits its columns in a stable order from run to run. #}
    {% set field_names = dbt_utils.get_column_values(ref('stg_zendesk__ticket_field_history'), 'field_name', default=[]) | sort %}

    {# Custom field ID -> title. max() keeps this deterministic when unioned connections disagree on the title
       for a given ID; the pivot emits one global set of columns, so a single name has to win either way. #}
    {% set titles_by_id = {} %}
    {% if var('using_ticket_custom_field', True) %}
        {% set custom_fields = run_query(
            "select cast(ticket_custom_field_id as " ~ dbt.type_string() ~ "), max(coalesce(title, raw_title))"
            ~ " from " ~ ref('stg_zendesk__ticket_custom_field')
            ~ " where coalesce(title, raw_title) is not null"
            ~ " group by 1"
        ) %}

        {# Positional access, since adapters differ on the casing of unquoted result column names. #}
        {% for row in custom_fields.rows %}
            {% do titles_by_id.update({row[0] | string: row[1]}) %}
        {% endfor %}
    {% endif %}

    {% set requested_slugs = [] %}
    {% for entry in var('ticket_field_history_columns') %}
        {% do requested_slugs.append(dbt_utils.slugify(entry)) %}
    {% endfor %}

    {# Keep the field_names that were requested by either their ID/name or their title, and name each column
       after its title where there is one. #}
    {% set aliases = {} %}
    {% for field_name in field_names %}
        {% set title = titles_by_id.get(field_name | string) %}
        {% if dbt_utils.slugify(field_name) in requested_slugs or (title and dbt_utils.slugify(title) in requested_slugs) %}
            {% do aliases.update({field_name: dbt_utils.slugify(title if title else field_name)}) %}
        {% endif %}
    {% endfor %}

    {% set alias_counts = {} %}
    {% for field_name, alias in aliases.items() %}
        {% do alias_counts.update({alias: alias_counts.get(alias, 0) + 1}) %}
    {% endfor %}

    {% for field_name, alias in aliases.items() %}
        {% if alias_counts[alias] > 1 %}
            {% set suffix = dbt_utils.slugify(field_name) %}
            {# slugify prepends an underscore to values starting with a digit, which would double up here. #}
            {% set suffix = suffix[1:] if suffix.startswith('_') else suffix %}
            {% do resolved_columns.update({field_name: alias ~ '_' ~ suffix}) %}
        {% else %}
            {% do resolved_columns.update({field_name: alias}) %}
        {% endif %}
    {% endfor %}

{% endif %}

{{ return(resolved_columns) }}

{% endmacro %}
