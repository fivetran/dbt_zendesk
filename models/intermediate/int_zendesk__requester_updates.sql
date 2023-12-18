with ticket_updates as (
    select *
    from {{ ref('int_zendesk__updates') }}

), ticket as (
    select *
    from {{ ref('stg_zendesk__ticket') }}

), ticket_requester as (
    select
        ticket.ticket_id,
        ticket.source_relation,
        ticket.requester_id,
        ticket_updates.valid_starting_at

    from ticket

    left join ticket_updates
        on ticket_updates.ticket_id = ticket.ticket_id
            and ticket_updates.user_id = ticket.requester_id
            and ticket_updates.source_relation = ticket.source_relation

), final as (
    select 
        ticket_id,
        source_relation,
        requester_id,
        max(valid_starting_at) as last_updated,
        count(*) as total_updates
    from ticket_requester

    group by 1, 2, 3
)

select * 
from final