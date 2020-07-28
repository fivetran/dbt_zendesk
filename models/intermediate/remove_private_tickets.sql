-- view of tickets that are created with private comments.  This is needed as a condition for determining first reply time calculations
with ticket_field_history as (

    select *
    from {{ ref('stg_zendesk_ticket_field_history') }}
    where field_name = 'is_public'


), ticket as (

    select *
    from {{ ref('stg_zendesk_ticket') }}

), joined as (

    select
        
        ticket.ticket_id,
        ticket_field_history.valid_ending_at < current_timestamp() as was_made_public,
        case when ticket_field_history.valid_ending_at < current_timestamp() 
            then ticket_field_history.valid_ending_at 
            else null end as made_public_at

    from ticket_field_history

    join ticket 
        on ticket.ticket_id = ticket_field_history.ticket_id
        and ticket.created_at = ticket_field_history.valid_starting_at
        and ticket_field_history.value = '0'

)
select *
from joined