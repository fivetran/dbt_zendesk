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
    ticket_created_at,
    sla_applied_at,
    sla_applied_at as sla_schedule_start_at,
    cast(null as timestamp) as sla_schedule_end_at,
    cast(null as {{ dbt.type_numeric() }}) as sum_lapsed_business_minutes,
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
    ticket_created_at,
    sla_applied_at,
    sla_schedule_start_at,
    sla_schedule_end_at,
    sum_lapsed_business_minutes,
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
    reply_time_breached_at.ticket_created_at,
    reply_time_breached_at.sla_applied_at,
    reply_time_breached_at.sum_lapsed_business_minutes,
    reply_time_breached_at.target,
    reply_time_breached_at.in_business_hours,
    min(reply_time_breached_at.sla_schedule_start_at) as sla_schedule_start_at,
    min(reply_time_breached_at.sla_schedule_end_at) as sla_schedule_end_at,
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
  {{ dbt_utils.group_by(n=8) }}

), lagging_time_block as (
  select
    *,
    lead(sla_schedule_start_at) over (partition by ticket_id, sla_policy_name, metric, sla_applied_at order by sla_schedule_start_at) as next_schedule_start,
    min(sla_breach_at) over (partition by sla_policy_name, metric, sla_applied_at order by sla_schedule_start_at rows unbounded preceding) as first_sla_breach_at,
		coalesce(lag(sum_lapsed_business_minutes) over (partition by sla_policy_name, metric, sla_applied_at order by sla_schedule_start_at), 0) as sum_lapsed_business_minutes_new
  from reply_time_breached_at_with_next_reply_timestamp

), filtered_reply_times as (
  select
    *
  from lagging_time_block
  where (
    in_business_hours
      and ((
        agent_reply_at >= sla_schedule_start_at and agent_reply_at <= sla_schedule_end_at) -- ticket is replied to between a schedule window
        or (agent_reply_at < sla_schedule_start_at and sum_lapsed_business_minutes_new = 0 and sla_breach_at = first_sla_breach_at) -- ticket is replied to before a schedule window and no business minutes have been spent on it
        or (agent_reply_at is null and {{ dbt.current_timestamp() }} >= sla_schedule_start_at and {{ dbt.current_timestamp() }} < next_schedule_start) -- ticket is not replied to and therefore active. But only bring through the active SLA record that is most recent (after the last SLA schedule starts but before the next)  
      ))
    or (not in_business_hours)

), reply_time_breached_at_remove_old_sla as (
  select
    *,
    {{ dbt.current_timestamp() }} as current_time_check,
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
  from filtered_reply_times
  
), reply_time_breach as (
  select 
    *,
    case when {{ dbt.datediff("sla_schedule_start_at", "agent_reply_at", 'minute') }} < 0 
      then 0 
      else sum_lapsed_business_minutes_new + {{ dbt.datediff("sla_schedule_start_at", "coalesce(agent_reply_at, current_time_check)", 'minute') }} 
    end as sla_elapsed_time
  from reply_time_breached_at_remove_old_sla
)

select *
from reply_time_breach