{% macro zendesk_staging_source_columns(tmp_relation, declared_columns) %}

{# Returns the columns fill_staging_columns should treat as present in the source.
   Default: ask the warehouse, and fill any missing declared column with nulls.
   Static mode: trust the declared list, so a missing source column fails the run instead of silently becoming null. #}

{% if var('zendesk_static_mode', false) %}
    {{ return(declared_columns) }}
{% else %}
    {{ return(adapter.get_columns_in_relation(tmp_relation)) }}
{% endif %}

{% endmacro %}


{% macro resolve_ticket_field_history_columns_static() %}

{# Static mode counterpart of resolve_ticket_field_history_columns: returns {field_name: column alias} built only from vars.
   Every requested field becomes a column, whether or not it has history yet, and custom field columns are named by
   ticket_field_history_custom_field_aliases instead of their current Zendesk title. #}

{% set custom_aliases = {} %}
{% for field_name, alias in var('ticket_field_history_custom_field_aliases', {}).items() %}
    {% do custom_aliases.update({field_name | string | lower: alias}) %}
{% endfor %}

{# History is keyed by field name or numeric ID, so an entry like a custom field title would only ever produce nulls. #}
{% for field_name in var('ticket_field_history_columns') | map('string') %}
    {% if not modules.re.match('^[a-z0-9_]+$', field_name) %}
        {{ exceptions.raise_compiler_error(
            "zendesk_static_mode: ticket_field_history_columns entry '" ~ field_name ~ "' is not a field name or numeric ID."
            ~ " List custom fields by ID and name them with ticket_field_history_custom_field_aliases."
        ) }}
    {% endif %}
{% endfor %}

{# Sorted so the column order matches what resolve_ticket_field_history_columns produces for the same fields. #}
{% set resolved_columns = {} %}
{% for field_name in var('ticket_field_history_columns') | map('string') | sort %}
    {% do resolved_columns.update({field_name: dbt_utils.slugify(custom_aliases.get(field_name | lower, field_name))}) %}
{% endfor %}

{# Aliases matching the models' own grain or date columns would collide or be dropped downstream. #}
{% set reserved_columns = ['source_relation', 'ticket_id', 'date_day', 'ticket_day_id', 'valid_from', 'valid_to', 'ending_day'] %}
{% for field_name, alias in resolved_columns.items() if alias in reserved_columns %}
    {{ exceptions.raise_compiler_error(
        "zendesk_static_mode: ticket_field_history_columns entry " ~ field_name ~ " resolves to the reserved column name '" ~ alias
        ~ "'. Give it a different alias in ticket_field_history_custom_field_aliases."
    ) }}
{% endfor %}

{# A duplicate alias would generate an invalid pivot, so fail at compile with the offending fields named. #}
{% set fields_by_alias = {} %}
{% for field_name, alias in resolved_columns.items() %}
    {% do fields_by_alias.update({alias: fields_by_alias.get(alias, []) + [field_name]}) %}
{% endfor %}
{% for alias, field_names in fields_by_alias.items() if field_names | length > 1 %}
    {{ exceptions.raise_compiler_error(
        "zendesk_static_mode: ticket_field_history_columns entries " ~ field_names | join(', ')
        ~ " all resolve to the column name '" ~ alias ~ "'. Give each a unique alias in ticket_field_history_custom_field_aliases."
    ) }}
{% endfor %}

{{ return(resolved_columns) }}

{% endmacro %}


{% macro get_ticket_field_history_value_columns(relation) %}

{# Returns the names of the pivoted field (and updater) columns that the field history models carry downstream.
   Default: read them off the upstream relation, so whatever the pivot physically has flows through.
   Static mode: derive them from the vars, the same way int_zendesk__field_history_pivot names them. #}

{% if var('zendesk_static_mode', false) %}
    {% set column_names = [] %}
    {% for col_xf in resolve_ticket_field_history_columns_static().values() %}
        {% do column_names.append(col_xf) %}
        {% for upd in var('ticket_field_history_updater_columns') %}
            {% do column_names.append(col_xf ~ '_' ~ upd) %}
        {% endfor %}
    {% endfor %}
    {{ return(column_names) }}

{% else %}
    {{ return(adapter.get_columns_in_relation(relation) | map(attribute='name') | list) }}

{% endif %}

{% endmacro %}
