{% macro resolve_ticket_field_history_columns() %}

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

    {# An entry is either a field ID, name, or title, or a {'field': ..., 'alias': ...} mapping that also names the column. #}
    {# The slug matches regardless of spacing and punctuation; the raw value matches titles with no Latin characters, which slugify to ''. #}
    {% set requested = [] %}
    {% for entry in var('ticket_field_history_columns') %}
        {% set field = (entry.get('field') if entry is mapping else entry) | string | trim %}
        {% set alias = entry.get('alias') if entry is mapping else none %}
        {% if alias and not modules.re.match('^[a-z_][a-z0-9_]*$', alias | string | lower) %}
            {{ exceptions.raise_compiler_error("ticket_field_history_columns alias '" ~ alias ~ "' for '" ~ field ~ "' is not a valid column name. Use only letters, numbers, and underscores, and don't start with a number.") }}
        {% endif %}
        {% do requested.append({'slug': zendesk_slugify(field), 'raw': field | lower, 'alias': (alias | string | lower) if alias else none}) %}
    {% endfor %}

    {# An entry that names a field_name directly (e.g. 'status') refers to that field, so it never matches a custom field title too. #}
    {% set field_name_slugs = [] %}
    {% for field_name in field_names %}
        {% do field_name_slugs.append(zendesk_slugify(field_name)) %}
    {% endfor %}

    {# Keep fields requested by ID/name or title, using the first matching entry. #}
    {% set aliases = {} %}
    {% set explicit_aliases = [] %}
    {% for field_name in field_names %}
        {% set field_name_slug = zendesk_slugify(field_name) %}
        {% set title = titles_by_id.get(field_name | string) %}
        {% set title_slug = zendesk_slugify(title) if title else '' %}
        {% set title_raw = (title | string | trim | lower) if title else none %}
        {% set match = [] %}
        {% for entry in requested if not match %}
            {# The `entry.slug and` guard stops two '' slugs from matching each other. #}
            {% if entry.slug == field_name_slug
                or (title and entry.slug not in field_name_slugs and ((entry.slug and entry.slug == title_slug) or entry.raw == title_raw)) %}
                {% do match.append(entry) %}
            {% endif %}
        {% endfor %}
        {% if match %}
            {% if match[0].alias %}
                {# Custom fields are recorded under their numeric ID, so this still works when using_ticket_custom_field is disabled. #}
                {% if not (field_name | string).isdigit() %}
                    {{ exceptions.raise_compiler_error("ticket_field_history_columns alias '" ~ match[0].alias ~ "' is set for the standard field '" ~ field_name ~ "'. Aliases are only supported for custom fields, since downstream models reference standard fields by name.") }}
                {% endif %}
                {% do explicit_aliases.append(field_name) %}
                {% do aliases.update({field_name: match[0].alias}) %}
            {# A title with no letters or numbers left after slugifying (e.g. 購入段階) falls back to the field ID, the same column as requesting it by ID. #}
            {% elif modules.re.search('[a-z0-9]', title_slug) %}
                {% do aliases.update({field_name: title_slug}) %}
            {% else %}
                {% do aliases.update({field_name: field_name_slug}) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# Two custom fields can share a title, or a custom field's title can match a standard field, which would generate duplicate column aliases. #}
    {% set alias_counts = {} %}
    {% for field_name, alias in aliases.items() %}
        {% do alias_counts.update({alias: alias_counts.get(alias, 0) + 1}) %}
    {% endfor %}

    {# Append the field_name to any custom field alias more than one field claims, to keep the column names unique. Standard fields keep their name so downstream references (e.g. status) still resolve. #}
    {% for field_name, alias in aliases.items() %}
        {% if alias_counts[alias] > 1 and field_name in explicit_aliases %}
            {{ exceptions.raise_compiler_error("ticket_field_history_columns alias '" ~ alias ~ "' for custom field " ~ field_name ~ " is already used by another tracked field. Choose a unique alias.") }}
        {% elif alias_counts[alias] > 1 and (field_name | string) in titles_by_id %}
            {% set suffix = zendesk_slugify(field_name) %}
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
