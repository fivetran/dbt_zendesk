{{ config(materialized='table') }}

-- Resolves each tracked ticket_field_history_columns entry to its human-readable name once, so downstream models
-- (int_zendesk__field_history_pivot, zendesk__ticket_backlog) don't each have to redo the same join. Standard
-- fields (status, priority, etc.) have no matching custom field row and just fall back to their own name.

with tracked_fields as (

    select distinct field_name
    from {{ ref('stg_zendesk__ticket_field_history') }}
    where lower(field_name) in ({{ "'" ~ (var('ticket_field_history_columns') | map('lower') | join("','")) ~ "'" }})

)

{% if var('using_ticket_custom_field', True) %}
, custom_fields as (

    select
        cast(ticket_custom_field_id as {{ dbt.type_string() }}) as ticket_custom_field_id,
        coalesce(title, raw_title) as resolved_name
    from {{ ref('stg_zendesk__ticket_custom_field') }}
    where coalesce(title, raw_title) is not null

), custom_fields_deduped as (

    -- A given custom field id could theoretically resolve to a different title across unioned source connections.
    -- max() keeps this deterministic rather than depending on row order.
    select
        ticket_custom_field_id,
        max(resolved_name) as resolved_name
    from custom_fields
    group by 1

)
{% endif %}

select
    tracked_fields.field_name
    {% if var('using_ticket_custom_field', True) -%}
    ,coalesce(custom_fields_deduped.resolved_name, tracked_fields.field_name) as resolved_name

from tracked_fields
left join custom_fields_deduped
    on cast(tracked_fields.field_name as {{ dbt.type_string() }}) = custom_fields_deduped.ticket_custom_field_id
    {%- else %}
    ,tracked_fields.field_name as resolved_name

from tracked_fields
    {%- endif %}
