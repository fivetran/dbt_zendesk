{% macro resolve_ticket_field_history_columns() %}

{# Returns a dict of {field_name: column alias} for each tracked ticket_field_history_columns entry. #}

{# Stays empty outside of a run/build (e.g. during dbt compile) so callers never have to guard the result. #}
{% set resolved_columns = {} %}

{% if execute and flags.WHICH in ('run', 'build') %}

    {# Every field_name present in the history data -- the pool an entry can match against. Sorted so the
       pivot emits its columns in a stable order from run to run. #}
    {% set field_names = dbt_utils.get_column_values(ref('stg_zendesk__ticket_field_history'), 'field_name', default=[]) | sort %}

    {# Custom field ID -> title, since Zendesk records custom field history under the ID but customers only
       ever see the title. max() keeps the result deterministic when unioned connections disagree on the title
       for a given ID; the pivot emits one global set of columns, so a single name has to win either way. #}
    {% set titles_by_id = {} %}
    {% if var('using_ticket_custom_field', True) %}
        {% set custom_field_query %}
            select
                cast(ticket_custom_field_id as {{ dbt.type_string() }}),
                max(coalesce(title, raw_title))
            from {{ ref('stg_zendesk__ticket_custom_field') }}
            where coalesce(title, raw_title) is not null
            group by 1
        {% endset %}

        {% set custom_fields = run_query(custom_field_query) %}

        {# Positional access, since adapters differ on the casing of unquoted result column names. #}
        {% for row in custom_fields.rows %}
            {% do titles_by_id.update({row[0] | string: row[1]}) %}
        {% endfor %}
    {% endif %}

    {# Slugify what the customer asked for so matching below ignores spacing and punctuation differences. #}
    {% set requested_slugs = [] %}
    {% for entry in var('ticket_field_history_columns') %}
        {% do requested_slugs.append(dbt_utils.slugify(entry)) %}
    {% endfor %}

    {# Keep the fields that were requested by either their ID/name or their title, and name each column after
       its title where there is one. Standard fields (status, priority, etc.) have no title and keep their own
       name. The `title and` guard matters: slugify(none) is an empty string, which would match every standard
       field if an entry in the variable also slugified to empty. #}
    {% set aliases = {} %}
    {% for field_name in field_names %}
        {% set title = titles_by_id.get(field_name | string) %}
        {% if dbt_utils.slugify(field_name) in requested_slugs or (title and dbt_utils.slugify(title) in requested_slugs) %}
            {% do aliases.update({field_name: dbt_utils.slugify(title if title else field_name)}) %}
        {% endif %}
    {% endfor %}

    {# Two custom fields can share a title, which would generate duplicate column aliases. #}
    {% set alias_counts = {} %}
    {% for field_name, alias in aliases.items() %}
        {% do alias_counts.update({alias: alias_counts.get(alias, 0) + 1}) %}
    {% endfor %}

    {# Append the field_name to any alias more than one field claims, to keep the column names unique. #}
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
