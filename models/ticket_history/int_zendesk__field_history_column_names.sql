{{ config(materialized='table') }}

-- Resolves each tracked ticket_field_history_columns entry to its human-readable name once, so downstream models
-- (int_zendesk__field_history_pivot, zendesk__ticket_backlog) don't each have to redo the same join. Standard
-- fields (status, priority, etc.) have no matching custom field row and just fall back to their own name.

with tracked_fields as (

    select distinct
        source_relation,
        field_name
    from {{ ref('stg_zendesk__ticket_field_history') }}
    where lower(field_name) in ({{ "'" ~ (var('ticket_field_history_columns') | map('lower') | join("','")) ~ "'" }})

)

{% if var('using_ticket_custom_field', True) %}
, custom_fields as (

    select
        source_relation,
        cast(ticket_custom_field_id as {{ dbt.type_string() }}) as ticket_custom_field_id,
        coalesce(title, raw_title) as resolved_name
    from {{ ref('stg_zendesk__ticket_custom_field') }}
    where coalesce(title, raw_title) is not null

), resolved_per_connection as (

    -- Scoped by source_relation so a custom field id from one unioned connection can't get matched against a
    -- different (and unrelated) custom field that happens to share the same id in another connection.
    select
        tracked_fields.field_name,
        coalesce(custom_fields.resolved_name, tracked_fields.field_name) as resolved_name
    from tracked_fields
    left join custom_fields
        on cast(tracked_fields.field_name as {{ dbt.type_string() }}) = custom_fields.ticket_custom_field_id
        and tracked_fields.source_relation = custom_fields.source_relation

), resolved_deduped as (

    -- The pivot's column list has to be one shared shape regardless of source_relation, so if two connections
    -- genuinely resolve the same field id to different names, max() picks one deterministically rather than
    -- depending on row order.
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
{% else %}
select distinct
    field_name,
    field_name as resolved_name
from tracked_fields
{% endif %}
