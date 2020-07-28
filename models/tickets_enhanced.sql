with ticket as (

    select *
    from {{ ref('stg_zendesk_ticket') }}

), user as (

    select *
    from {{ ref('stg_zendesk_user') }}

), joined as (

    select 

        ticket.*,
        requester.role as requester_role,
        requester.role = 'agent' as agent_created_ticket,
        requester.email as requester_email,
        submitter.role as submitter_role,
        submitter.email as submitter_email,

    
    from ticket

    join user as requester
        on requester.user_id = ticket.requester_id
    
    join user as submitter
        on submitter.user_id = ticket.submitter_id
)

select *
from joined