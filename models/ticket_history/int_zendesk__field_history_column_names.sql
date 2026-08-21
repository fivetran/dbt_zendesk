{{ config(materialized='table') }}

-- Resolves each tracked ticket_field_history_columns entry to its human-readable name once, so downstream models
-- (int_zendesk__field_history_pivot, zendesk__ticket_backlog) don't each have to redo the same join. Standard
-- fields (status, priority, etc.) have no matching custom field row and just fall back to their own name. Custom
-- fields can be referenced in ticket_field_history_columns by either their numeric id or their title/raw_title.

with tracked_field_history_names as (

    select distinct
        source_relation,
        cast(field_name as {{ dbt.type_string() }}) as field_name
    from {{ ref('stg_zendesk__ticket_field_history') }}

)

{% if var('using_ticket_custom_field', True) %}
, custom_fields as (

    select
        source_relation,
        cast(ticket_custom_field_id as {{ dbt.type_string() }}) as ticket_custom_field_id,
        coalesce(title, raw_title) as resolved_name
    from {{ ref('stg_zendesk__ticket_custom_field') }}
    where coalesce(title, raw_title) is not null

)
{% endif %}

, tracked_entries as (

    {% for entry in var('ticket_field_history_columns') %}
    select lower('{{ entry | replace("'", "''") }}') as entry_value_lower
    {% if not loop.last %} union all {% endif %}
    {% endfor %}

), matched as (

    -- Scoped by source_relation so a custom field id/title from one unioned connection can't get matched against
    -- a different (and unrelated) custom field that happens to share the same id/title in another connection.
    select distinct
        tracked_field_history_names.source_relation,
        tracked_field_history_names.field_name,
        {% if var('using_ticket_custom_field', True) -%}
        custom_fields.resolved_name as custom_resolved_name
        {%- else -%}
        cast(null as {{ dbt.type_string() }}) as custom_resolved_name
        {%- endif %}
    from tracked_field_history_names
    {% if var('using_ticket_custom_field', True) %}
    left join custom_fields
        on tracked_field_history_names.field_name = custom_fields.ticket_custom_field_id
        and tracked_field_history_names.source_relation = custom_fields.source_relation
    {% endif %}
    inner join tracked_entries
        on lower(tracked_field_history_names.field_name) = tracked_entries.entry_value_lower
        {% if var('using_ticket_custom_field', True) -%}
        or (custom_fields.resolved_name is not null and lower(custom_fields.resolved_name) = tracked_entries.entry_value_lower)
        {%- endif %}

), resolved_per_connection as (

    select
        source_relation,
        field_name,
        coalesce(custom_resolved_name, field_name) as resolved_name
    from matched

), resolved_deduped as (

    -- The pivot's column list has to be one shared shape regardless of source_relation, so if two connections
    -- genuinely resolve the same field id/title to different names, max() picks one deterministically rather
    -- than depending on row order.
    select
        field_name,
        max(resolved_name) as resolved_name
    from resolved_per_connection
    group by 1

)

select
    field_name,
    resolved_name
from resolved_deduped
