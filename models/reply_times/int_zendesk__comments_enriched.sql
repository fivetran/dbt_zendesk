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
    /*
    In int_zendesk__ticket_reply_times we will only be focusing on reply times between public tickets.
    The below union explicitly identifies the previous commenter roles of public and not public comments.
    */
    select
        *,
        coalesce(
            lag(commenter_role) over (partition by ticket_id order by valid_starting_at, commenter_role)
            , 'first_comment') 
            as previous_commenter_role
    from joined
    where is_public

    union all

    select
        *,
        'non_public_comment' as previous_commenter_role
    from joined
    where not is_public
)

select 
    *,
    first_value(valid_starting_at) over (partition by ticket_id order by valid_starting_at desc, ticket_id rows unbounded preceding) as last_comment_added_at,
    sum(case when not is_public then 1 else 0 end) over (partition by ticket_id order by valid_starting_at rows between unbounded preceding and current row) as previous_internal_comment_count
from add_previous_commenter_role