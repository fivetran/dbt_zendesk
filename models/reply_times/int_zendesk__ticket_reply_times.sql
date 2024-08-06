{{ config(enabled=var('customer360__using_zendesk', true)) }}

with ticket_public_comments as (

    select *
    from {{ ref('int_zendesk__comments_enriched') }}
    where is_public

), end_user_comments as (
  
  select 
    ticket_id,
    valid_starting_at as end_user_comment_created_at,
    ticket_created_date,
    commenter_role,
    previous_internal_comment_count,
    previous_commenter_role = 'first_comment' as is_first_comment
  from ticket_public_comments 
  where (commenter_role = 'external_comment'
    and ticket_public_comments.previous_commenter_role != 'external_comment') -- we only care about net new end user comments
    or previous_commenter_role = 'first_comment' -- We also want to take into consideration internal first comment replies

), reply_timestamps as (  

  select
    end_user_comments.ticket_id,
    -- If the commentor was internal, a first comment, and had previous non public internal comments then we want the ticket created date to be the end user comment created date
    -- Otherwise we will want to end user comment created date
    case when is_first_comment then end_user_comments.ticket_created_date else end_user_comments.end_user_comment_created_at end as end_user_comment_created_at,
    end_user_comments.is_first_comment,
    min(case when is_first_comment 
        and end_user_comments.commenter_role != 'external_comment' 
        and (end_user_comments.previous_internal_comment_count > 0)
          then end_user_comments.end_user_comment_created_at 
        else agent_comments.valid_starting_at end) as agent_responded_at
  from end_user_comments
  left join ticket_public_comments as agent_comments
    on agent_comments.ticket_id = end_user_comments.ticket_id
    and agent_comments.commenter_role = 'internal_comment'
    and agent_comments.valid_starting_at > end_user_comments.end_user_comment_created_at
  group by 1,2,3

)

  select
    *,
    ({{ dbt.datediff(
      'end_user_comment_created_at',
      'agent_responded_at',
      'second') }} / 60) as reply_time_calendar_minutes
  from reply_timestamps
  order by 1,2