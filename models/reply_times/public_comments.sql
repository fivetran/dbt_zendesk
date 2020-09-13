with public_ticket_comment as (

    select *
    from {{ ref('stg_zendesk_ticket_comment') }}
    where is_public

), users as (

    select *
    from {{ ref('stg_zendesk_user') }}

), joined as (

    select 

        public_ticket_comment.*,
        case when commenter.role = 'end-user' then 'external_comment'
            when commenter.role in ('agent','admin') then 'internal_comment'
            else 'unknown' end as commenter_role
    
    from public_ticket_comment
    
    join users as commenter
        on commenter.user_id = public_ticket_comment.user_id

), add_previous_commenter_role as (

    select
        *,
        coalesce(
            lag(commenter_role) over (partition by ticket_id order by created_at)
            , 'first_comment') 
            as previous_commenter_role

    from joined
)

select * 
from add_previous_commenter_role