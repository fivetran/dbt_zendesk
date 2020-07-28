with public_ticket_comment as (

    select *
    from {{ ref('stg_zendesk_ticket_comment') }}
    where is_public

), user as (

    select *
    from {{ ref('stg_zendesk_user') }}

), ticket as (

    select *
    from {{ ref('stg_zendesk_ticket') }}

), joined as (

    select 

        public_ticket_comment.*,
        commenter.role as commenter_role,
        coalesce(
                lag(commenter.role) over (partition by public_ticket_comment.ticket_id  order by public_ticket_comment.created_at)
                , 'first_comment') 
                as previous_commenter_role,
        row_number() over (partition by public_ticket_comment.ticket_id order by public_ticket_comment.created_at) as public_comment_counter,
        case when role = 'agent' 
            then (row_number() over (partition by public_ticket_comment.ticket_id, role order by public_ticket_comment.created_at))
          else null end as agent_public_comment_counter,
       case when role = 'end-user' 
            then (row_number() over (partition by public_ticket_comment.ticket_id, role order by public_ticket_comment.created_at))
          else null end as end_user_public_comment_counter 
    
    from public_ticket_comment
    
    join user as commenter
        on commenter.user_id = public_ticket_comment.user_id
    
    join ticket
        on ticket.ticket_id = public_ticket_comment.ticket_id

)
select * 
from joined