{% macro resolve_ticket_field_history_columns() %}
  {{ return(adapter.dispatch('resolve_ticket_field_history_columns', 'zendesk')()) }}
{% endmacro %}

{% macro default__resolve_ticket_field_history_columns() %}

{# Returns a dict of {field_name: column alias} for each tracked ticket_field_history_columns entry. #}

{# Stays empty outside of a run/build (e.g. during dbt compile) so callers never have to guard the result. #}
{% set resolved_columns = {} %}

{% if execute and flags.WHICH in ('run', 'build') %}

    {# Every field_name in the history data, sorted so the pivot's column order is stable across runs. #}
    {% set field_names = dbt_utils.get_column_values(ref('stg_zendesk__ticket_field_history'), 'field_name', default=[]) | sort %}

    {# Custom field ID -> title, since history is recorded under the ID; max() breaks ties across connections. #}
    {% set titles_by_id = {} %}
    {% if var('using_ticket_custom_field', True) %}
        {% set custom_field_query %}
            select
                cast(ticket_custom_field_id as {{ dbt.type_string() }}),
                max(title)
            from {{ ref('stg_zendesk__ticket_custom_field') }}
            where title is not null
            group by 1
        {% endset %}

        {% set custom_fields = run_query(custom_field_query) %}

        {# Positional access, since adapters differ on the casing of unquoted result column names. #}
        {% for row in custom_fields.rows %}
            {% do titles_by_id.update({row[0] | string: row[1]}) %}
        {% endfor %}
    {% endif %}

    {# An entry that names a field_name directly (e.g. 'status') refers to that field, so it never matches a custom field title too. #}
    {% set field_name_slugs = [] %}
    {% for field_name in field_names %}
        {% do field_name_slugs.append(zendesk.zendesk_slugify(field_name)) %}
    {% endfor %}

    {# Slugify each entry, so matching ignores spacing and punctuation. Also keep the raw entry, to match titles with no Latin letters or numbers (e.g. 購入段階), which slugify to ''. #}
    {% set requested_slugs = [] %}
    {% set requested_title_raws = [] %}
    {% for entry in var('ticket_field_history_columns') %}
        {% set entry_slug = zendesk.zendesk_slugify(entry) %}
        {% do requested_slugs.append(entry_slug) %}
        {% if entry_slug not in field_name_slugs %}
            {% do requested_title_raws.append(entry | string | trim | lower) %}
        {% endif %}
    {% endfor %}

    {# Keep fields requested by ID/name or title. The `title_slug and` guard stops an empty slug from matching other empty slugs. #}
    {% set aliases = {} %}
    {% for field_name in field_names %}
        {% set field_name_slug = zendesk.zendesk_slugify(field_name) %}
        {% set title = titles_by_id.get(field_name | string) %}
        {% set title_slug = zendesk.zendesk_slugify(title) if title else '' %}
        {% set title_raw = (title | string | trim | lower) if title else none %}
        {% set title_requested = title and ((title_slug and title_slug in requested_slugs and title_slug not in field_name_slugs) or title_raw in requested_title_raws) %}
        {% if field_name_slug in requested_slugs or title_requested %}
            {# A title with no letters or numbers left after slugifying falls back to the field ID, the same column as requesting it by ID. #}
            {% do aliases.update({field_name: title_slug if modules.re.search('[a-z0-9]', title_slug) else field_name_slug}) %}
        {% endif %}
    {% endfor %}

    {# Two custom fields can share a title, or a custom field's title can match a standard field, which would generate duplicate column aliases. #}
    {% set alias_counts = {} %}
    {% for field_name, alias in aliases.items() %}
        {% do alias_counts.update({alias: alias_counts.get(alias, 0) + 1}) %}
    {% endfor %}

    {# Append the field_name to any custom field alias more than one field claims, to keep the column names unique. Standard fields keep their name so downstream references (e.g. status) still resolve. #}
    {% for field_name, alias in aliases.items() %}
        {% if alias_counts[alias] > 1 and (field_name | string) in titles_by_id %}
            {% set suffix = zendesk.zendesk_slugify(field_name) %}
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
