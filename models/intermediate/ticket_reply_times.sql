with tickets_enhanced as (

    select *
    from {{ ref('tickets_enhanced') }}

), ticket_public_comments as (

    select *
    from {{ ref('public_comments') }}

), end_user_comments as (
  
  select 
    ticket_id,
    created_at as end_user_comment_created_at,
    previous_commenter_role = 'first_comment' as is_first_comment
  from ticket_public_comments 
  where commenter_role = 'end-user'
    and ticket_public_comments.previous_commenter_role != 'end-user' -- we only care about net new end user comments

), reply_timestamps as (  

  select 
    end_user_comments.*,
    min(agent_comments.created_at) as agent_responsed_at
  from end_user_comments
  left join ticket_public_comments as agent_comments
    on agent_comments.ticket_id = end_user_comments.ticket_id
    and agent_comments.commenter_role in ('agent','admin')
    and agent_comments.previous_commenter_role not in ('agent','admin') -- we only care about net new agent comments
    and agent_comments.created_at > end_user_comments.end_user_comment_created
  group by 1,2,3

)

  select
    *,
    timestamp_diff(agent_response_timestamp,end_user_comment_created, minute) as reply_time_calendar_minutes
  from reply_timestamps
  order by 1,2
