with ticket_comment as (

    select *
    from {{ ref('int_zendesk__updates') }}
    where field_name = 'comment'

), users as (

    select *
    from {{ ref('stg_zendesk__user') }}

), joined as (

    select 

        ticket_comment.*,
        case when commenter.role = 'end-user' then 'external_comment'
            when commenter.role in ('agent','admin') then 'internal_comment'
            else 'unknown' end as commenter_role
    
    from ticket_comment
    
    join users as commenter
        on commenter.user_id = ticket_comment.user_id

), add_previous_commenter_role as (

    select
        *,
        coalesce(
            lag(commenter_role) over (partition by ticket_id order by valid_starting_at)
            , 'first_comment') 
            as previous_commenter_role,
        first_value(valid_starting_at) over (partition by ticket_id order by valid_starting_at desc, ticket_id rows unbounded preceding) as last_comment_added_at

    from joined
)

select * 
from add_previous_commenter_role