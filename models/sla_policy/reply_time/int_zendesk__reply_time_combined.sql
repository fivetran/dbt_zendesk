with reply_time_calendar_hours_sla as (
  
  select *
  from {{ ref('int_zendesk__reply_time_calendar_hours') }}

{% if var('using_schedules', True) %}

), reply_time_business_hours_sla as (
 
  select *
  from {{ ref('int_zendesk__reply_time_business_hours') }}

{% endif %}

), ticket_updates as (
  select *
  from {{ ref('int_zendesk__updates') }}

), users as (
 
  select *
  from {{ ref('int_zendesk__user_aggregates') }}

), reply_time_breached_at as (

  select 
    ticket_id,
    sla_policy_name,
    metric,
    sla_applied_at,
    target,
    in_business_hours,
    sla_breach_at
  from reply_time_calendar_hours_sla

{% if var('using_schedules', True) %}

  union all

  select 
    ticket_id,
    sla_policy_name,
    metric,
    sla_applied_at,
    target,
    in_business_hours,
    sla_breach_at
  from reply_time_business_hours_sla
{% endif %}

-- Now that we have the breach time, see when the first reply after the sla policy was applied took place.
), ticket_solved_times as (
  select
    ticket_id,
    valid_starting_at as solved_at
  from ticket_updates
  where field_name = 'status'
  and value in ('solved','closed')

), reply_time as (
    select 
      ticket_comment.ticket_id,
      ticket_comment.valid_starting_at as reply_at,
      commenter.role
    from ticket_updates as ticket_comment
    join users as commenter
      on commenter.user_id = ticket_comment.user_id
    where field_name = 'comment' 
      and ticket_comment.is_public
      and commenter.role in ('agent','admin')

), reply_time_breached_at_with_next_reply_timestamp as (

  select 
    reply_time_breached_at.ticket_id,
    reply_time_breached_at.sla_policy_name,
    reply_time_breached_at.metric,
    reply_time_breached_at.sla_applied_at,
    reply_time_breached_at.target,
    reply_time_breached_at.in_business_hours,
    min(sla_breach_at) as sla_breach_at,
    min(reply_at) as agent_reply_at,
    min(solved_at) as next_solved_at
  from reply_time_breached_at
  left join reply_time
    on reply_time.ticket_id = reply_time_breached_at.ticket_id
    and reply_time.reply_at > reply_time_breached_at.sla_applied_at
  left join ticket_solved_times
    on reply_time_breached_at.ticket_id = ticket_solved_times.ticket_id
    and ticket_solved_times.solved_at > reply_time_breached_at.sla_applied_at
  group by 1, 2, 3, 4, 5, 6

), reply_time_breached_at_remove_old_sla as (
  select 
    *,
    lead(sla_applied_at) over (partition by ticket_id, metric, in_business_hours order by sla_applied_at) as updated_sla_policy_starts_at,
    case when 
      lead(sla_applied_at) over (partition by ticket_id, metric, in_business_hours order by sla_applied_at) --updated sla policy start at time
      < sla_breach_at then true else false end as is_stale_sla_policy,
    case when (sla_breach_at < agent_reply_at and sla_breach_at < next_solved_at)
                or (sla_breach_at < agent_reply_at and next_solved_at is null)
                or (agent_reply_at is null and sla_breach_at < next_solved_at)
                or (agent_reply_at is null and next_solved_at is null)
      then true
      else false
        end as is_sla_breached
  from reply_time_breached_at_with_next_reply_timestamp
  
), reply_time_breach as (
  select 
    *,
    {{ dbt_utils.datediff("sla_applied_at", "agent_reply_at", 'minute') }} as sla_elapsed_time
  from reply_time_breached_at_remove_old_sla
)

select *
from reply_time_breach