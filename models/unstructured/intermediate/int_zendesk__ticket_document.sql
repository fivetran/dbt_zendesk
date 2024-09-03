{{ config(enabled=var('zendesk__unstructured_enabled', False)) }}

with tickets as (
    select *
    from {{ var('ticket') }}

), users as (
    select *
    from {{ var('user') }}

), ticket_details as (
    select
        tickets.ticket_id,
        tickets.subject AS ticket_name,
        {{ zendesk.coalesce_cast(["users.name", "'UNKNOWN'"], dbt.type_string()) }} as user_name,
        {{ zendesk.coalesce_cast(["users.email", "'UNKNOWN'"], dbt.type_string()) }} as created_by,
        tickets.created_at AS created_on,
        {{ zendesk.coalesce_cast(["tickets.status", "'UNKNOWN'"], dbt.type_string()) }} as status,
        {{ zendesk.coalesce_cast(["tickets.priority", "'UNKNOWN'"], dbt.type_string()) }} as priority
    from tickets
    left join users
        on tickets.requester_id = users.user_id
    where not coalesce(tickets._fivetran_deleted, False)
        and not coalesce(users._fivetran_deleted, False)

), final as (
    select
        ticket_id,
        {{ dbt.concat([
            "'# Ticket : '", "ticket_name", "'\\n\\n'",
            "'Created By : '", "user_name", "' ('", "created_by", "')\\n'",
            "'Created On : '", "created_on", "'\\n'",
            "'Status : '", "status", "'\\n'",
            "'Priority : '", "priority"
        ]) }} as ticket_markdown
    from ticket_details
)

select 
    *,
    {{ zendesk.count_tokens("ticket_markdown") }} as ticket_tokens
from final