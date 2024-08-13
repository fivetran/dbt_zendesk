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
        coalesce(users.name, 'UNKNOWN') AS user_name,
        coalesce(users.email, 'UNKNOWN') AS created_by,
        tickets.created_at AS created_on,
        coalesce(tickets.status, 'UNKNOWN') as status,
        coalesce(tickets.priority, 'UNKNOWN') as priority
    from tickets
    left join users
        on tickets.requester_id = users.user_id

    {# where tickets._fivetran_deleted = False -- _fivetran_deleted add to source
        and users._fivetran_deleted = False -- _fivetran_deleted add to source #}

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