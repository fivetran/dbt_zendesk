{% macro resolve_ticket_field_history_columns() %}
  {{ return(adapter.dispatch('resolve_ticket_field_history_columns', 'zendesk')()) }}
{% endmacro %}

{% macro default__resolve_ticket_field_history_columns() %}

{# Returns {field_name: column alias} for each tracked field; empty outside run/build. #}
{% set resolved_columns = {} %}

{% if execute and flags.WHICH in ('run', 'build') %}

    {# Sorted so the pivot's column order is stable. #}
    {% set field_names = dbt_utils.get_column_values(ref('stg_zendesk__ticket_field_history'), 'field_name', default=[]) | sort %}

    {# Custom field ID -> title; max() breaks ties across connections. #}
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

        {# Positional access, since adapters differ on result column casing. #}
        {% for row in custom_fields.rows %}
            {% do titles_by_id.update({row[0] | string: row[1]}) %}
        {% endfor %}
    {% endif %}

    {# Skips entries with no Latin letters or numbers (e.g. 購入段階), which dbt_utils.slugify fails on. #}
    {% set requested_slugs = [] %}
    {% for entry in var('ticket_field_history_columns') if modules.re.search('[A-Za-z0-9]', entry | string) %}
        {% do requested_slugs.append(dbt_utils.slugify(entry)) %}
    {% endfor %}

    {# An entry naming a field_name (e.g. 'status') never also matches a custom field title. #}
    {% set field_name_slugs = [] %}
    {% for field_name in field_names %}
        {% do field_name_slugs.append(dbt_utils.slugify(field_name)) %}
    {% endfor %}

    {# Match by ID/name or title; non-Latin titles are skipped, so those fields match by ID only. #}
    {% set aliases = {} %}
    {% for field_name in field_names %}
        {% set title = titles_by_id.get(field_name | string) %}
        {% set title_slug = dbt_utils.slugify(title) if title and modules.re.search('[A-Za-z0-9]', title | string) else none %}
        {% if dbt_utils.slugify(field_name) in requested_slugs or (title_slug and title_slug in requested_slugs and title_slug not in field_name_slugs) %}
            {% do aliases.update({field_name: title_slug if title_slug else dbt_utils.slugify(field_name)}) %}
        {% endif %}
    {% endfor %}

    {# Counts aliases, since custom fields can share a title with each other or a standard field. #}
    {% set alias_counts = {} %}
    {% for field_name, alias in aliases.items() %}
        {% do alias_counts.update({alias: alias_counts.get(alias, 0) + 1}) %}
    {% endfor %}

    {# Suffixes duplicate custom field aliases with the ID; standard fields keep their name. #}
    {% for field_name, alias in aliases.items() %}
        {% if alias_counts[alias] > 1 and (field_name | string) in titles_by_id %}
            {% set suffix = dbt_utils.slugify(field_name) %}
            {# slugify turns an ID into _123, so this drops the _ to avoid status__123. #}
            {% set suffix = suffix[1:] if suffix.startswith('_') else suffix %}
            {% do resolved_columns.update({field_name: alias ~ '_' ~ suffix}) %}
        {% else %}
            {% do resolved_columns.update({field_name: alias}) %}
        {% endif %}
    {% endfor %}

{% endif %}

{{ return(resolved_columns) }}

{% endmacro %}
