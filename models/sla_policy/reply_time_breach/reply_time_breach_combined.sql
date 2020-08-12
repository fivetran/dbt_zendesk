with reply_time_calendar_hours_breached as (
  
  select *
  from {{ ref('reply_time_calendar_hours_breached') }}

), reply_time_business_hours_breached as (
 
  select *
  from {{ ref('reply_time_business_hours_breached') }}

), ticket_field_history as (
 
  select *
  from {{ ref('stg_zendesk_ticket_field_history') }}

), ticket_comment as (
 
  select *
  from {{ ref('stg_zendesk_ticket_comment') }}

), user as (
 
  select *
  from {{ ref('stg_zendesk_user') }}



), reply_time_breached_at as (

  select 
    ticket_id,
    metric,
    sla_applied_at,
    target,
    in_business_hours,
    breached_at
  from reply_time_calendar_hours_breached

  union all

  select 
    *
  from reply_time_business_hours_breached

-- Now that we have the breach time, see when the first reply after the sla policy was applied took place.
), ticket_solved_times as (
  select
    ticket_id,
    valid_starting_at as solved_at
  from ticket_field_history
  where field_name = 'status'
  and value in ('solved','closed')

), reply_time as (
    select 
      ticket_comment.ticket_id,
      ticket_comment.created_at as reply_at,
      commenter.role
    from ticket_comment
    join user as commenter
      on commenter.user_id = ticket_comment.user_id
    where ticket_comment.is_public
    and commenter.role in ('agent','admin')

), reply_time_breached_at_with_next_reply_timestamp as (

  select 
    reply_time_breached_at.*,
    min(reply_at) as agent_reply_at,
    min(solved_at) as next_solved_at
  from reply_time_breached_at
  left join reply_time
    on reply_time.ticket_id = reply_time_breached_at.ticket_id
    and reply_time.reply_at > reply_time_breached_at.sla_applied_at
  left join ticket_solved_times
    on reply_time_breached_at.ticket_id = ticket_solved_times.ticket_id
    and ticket_solved_times.solved_at > reply_time_breached_at.sla_applied_at
  group by 1,2,3,4,5,6

), reply_time_breached_at_remove_old_sla as (
  select 
    *,
    lead(sla_applied_at) over (partition by ticket_id, metric, in_business_hours order by sla_applied_at) as updated_sla_policy_starts_at,
    case when 
      lead(sla_applied_at) over (partition by ticket_id, metric, in_business_hours order by sla_applied_at) --updated sla policy start at time
      < breached_at then true else false end as is_stale_sla_policy
  from reply_time_breached_at_with_next_reply_timestamp
  
-- final query that filters out tickets that were solved or replied to before breach time
), reply_time_breach as (
  select 
    * 
  from reply_time_breached_at_remove_old_sla
  where (breached_at < agent_reply_at and breached_at < next_solved_at)
    or (breached_at < agent_reply_at and next_solved_at is null)
    or (agent_reply_at is null and breached_at < next_solved_at)
    or (agent_reply_at is null and next_solved_at is null)    

)

select *
from reply_time_breach