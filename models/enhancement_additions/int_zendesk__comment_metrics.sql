with ticket_comments as (

    select *
    from {{ ref('stg_zendesk__ticket_comment') }}

),  user_table as (

    select *
    from {{ ref('stg_zendesk__user') }}
),

comment_counts as (
    select
        ticket_comments.ticket_id,
        sum(case when lower(user_table.role) != 'end-user'
            then 1
            else 0
                end) as count_agent_comments,
        sum(case when lower(user_table.role) = 'end-user'
            then 1
            else 0
                end) as count_end_user_comments,
        sum(case when ticket_comments.is_public = true
            then 1
            else 0
                end) as count_public_comments,
        sum(case when ticket_comments.is_public = false
            then 1
            else 0
                end) as count_internal_comments,
        count(*) as total_comments
    from ticket_comments

    left join user_table
        using (user_id)

    group by 1
)

select * 
from comment_counts