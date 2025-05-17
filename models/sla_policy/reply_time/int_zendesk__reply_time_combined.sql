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

{% set using_user_role_histories = var('using_user_role_histories', True) and var('using_audit_log', False) %}
{% if using_user_role_histories %}
), user_role_history as (

  select *
  from {{ ref('int_zendesk__user_role_history') }}
{% endif %}

), reply_time_breached_at as (

  select 
    source_relation,
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
    sla_breach_at,
    cast(null as {{ dbt.type_numeric() }}) as week_number,
    cast(null as {{ dbt.type_numeric() }}) as total_schedule_weekly_business_minutes
  from reply_time_calendar_hours_sla

{% if var('using_schedules', True) %}

  union all

  select 
    source_relation,
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
    sla_breach_exact_time as sla_breach_at,
    week_number,
    total_schedule_weekly_business_minutes
  from reply_time_business_hours_sla
{% endif %}

-- Now that we have the breach time, see when the first reply after the sla policy was applied took place.
), ticket_solved_times as (
  select
    source_relation,
    ticket_id,
    valid_starting_at as solved_at
  from ticket_updates
  where field_name = 'status'
  and value in ('solved','closed')

), reply_time as (
  select 
    ticket_comment.source_relation,
    ticket_comment.ticket_id,
    ticket_comment.valid_starting_at as reply_at,
    {{ "user_role_history.role" if using_user_role_histories else "commenter.role" }}
  from ticket_updates as ticket_comment

  join users as commenter
    on commenter.user_id = ticket_comment.user_id
    and commenter.source_relation = ticket_comment.source_relation

  {% if using_user_role_histories %}
  left join user_role_history
    on user_role_history.user_id = commenter.user_id
    and user_role_history.source_relation = commenter.source_relation
    and ticket_comment.valid_starting_at >= user_role_history.valid_starting_at
    and ticket_comment.valid_starting_at < user_role_history.valid_ending_at 
  {% endif %}

  where field_name = 'comment' 
    and ticket_comment.is_public
    and {{"user_role_history.is_internal_role" if using_user_role_histories else "commenter.role in ('agent','admin')" }}

), reply_time_breached_at_with_next_reply_timestamp as (

  select 
    reply_time_breached_at.source_relation,
    reply_time_breached_at.ticket_id,
    reply_time_breached_at.sla_policy_name,
    reply_time_breached_at.metric,
    reply_time_breached_at.ticket_created_at,
    reply_time_breached_at.sla_applied_at,
    reply_time_breached_at.sum_lapsed_business_minutes,
    reply_time_breached_at.target,
    reply_time_breached_at.in_business_hours,
    reply_time_breached_at.sla_breach_at,
    reply_time_breached_at.week_number,
    min(reply_time_breached_at.sla_schedule_start_at) as sla_schedule_start_at,
    min(reply_time_breached_at.sla_schedule_end_at) as sla_schedule_end_at,
    min(reply_at) as agent_reply_at,
    min(solved_at) as next_solved_at
  from reply_time_breached_at
  left join reply_time
    on reply_time.ticket_id = reply_time_breached_at.ticket_id
    and reply_time.reply_at > reply_time_breached_at.sla_applied_at
    and reply_time.source_relation = reply_time_breached_at.source_relation
  left join ticket_solved_times
    on reply_time_breached_at.ticket_id = ticket_solved_times.ticket_id
    and ticket_solved_times.solved_at > reply_time_breached_at.sla_applied_at
    and ticket_solved_times.source_relation = reply_time_breached_at.source_relation
  {{ dbt_utils.group_by(n=11) }}

), lagging_time_block as (
  select
    *,
    row_number() over (partition by source_relation, ticket_id, metric, sla_applied_at order by sla_schedule_start_at) as day_index,
    lead(sla_schedule_start_at) over (partition by source_relation, ticket_id, sla_policy_name, metric, sla_applied_at order by sla_schedule_start_at) as next_schedule_start,
    min(sla_breach_at) over (partition by source_relation, sla_policy_name, metric, sla_applied_at order by sla_schedule_start_at rows unbounded preceding) as first_sla_breach_at,
		coalesce(lag(sum_lapsed_business_minutes) over (partition by source_relation, sla_policy_name, metric, sla_applied_at order by sla_schedule_start_at), 0) as sum_lapsed_business_minutes_new,
    {{ dbt.datediff("sla_schedule_start_at", "agent_reply_at", 'second') }} / 60 as total_runtime_minutes -- total minutes from sla_schedule_start_at and agent reply time, before taking into account SLA end time
  from reply_time_breached_at_with_next_reply_timestamp

), filtered_reply_times as (
  select
    *
  from lagging_time_block
  where (
    in_business_hours
      and ((
        agent_reply_at >= sla_schedule_start_at and agent_reply_at <= sla_schedule_end_at) -- ticket is replied to between a schedule window
        or (agent_reply_at < sla_schedule_start_at and sum_lapsed_business_minutes_new = 0 and sla_breach_at = first_sla_breach_at and day_index = 1) -- ticket is replied to before any schedule begins and no business minutes have been spent on it
        or (agent_reply_at is null and next_solved_at >= sla_schedule_start_at and next_solved_at < next_schedule_start) -- There are no reply times, but the ticket is closed and we should capture the closed date as the first and/or next reply time if there is not one preceding.
        or (next_solved_at is null and agent_reply_at is null and {{ dbt.current_timestamp() }} >= sla_schedule_start_at and ({{ dbt.current_timestamp() }} < next_schedule_start or next_schedule_start is null)) -- ticket is not replied to and therefore active. But only bring through the active SLA record that is most recent (after the last SLA schedule starts but before the next, or if there does not exist a next SLA schedule start time)  
        or (agent_reply_at > sla_schedule_end_at and (agent_reply_at < next_schedule_start or next_schedule_start is null)) -- ticket is replied to outside sla schedule hours
      ) and sla_schedule_start_at <= {{ dbt.current_timestamp() }}) -- To help limit the data we do not want to bring through any schedule rows in the future.
    or not in_business_hours

), reply_time_breached_at_remove_old_sla as (
  select
    *,
    {{ dbt.current_timestamp() }} as current_time_check,
    lead(sla_applied_at) over (partition by source_relation, ticket_id, metric, in_business_hours order by sla_applied_at) as updated_sla_policy_starts_at,
    case when 
      lead(sla_applied_at) over (partition by source_relation, ticket_id, metric, in_business_hours order by sla_applied_at) --updated sla policy start at time
      < sla_breach_at then true else false end as is_stale_sla_policy,
    case when (sla_breach_at < agent_reply_at and sla_breach_at < next_solved_at)
                or (sla_breach_at < agent_reply_at and next_solved_at is null)
                or (agent_reply_at is null and sla_breach_at < next_solved_at)
                or (agent_reply_at is null and next_solved_at is null)
      then true
      else false
        end as is_sla_breached,
    sum_lapsed_business_minutes_new + total_runtime_minutes as total_new_minutes -- add total runtime to sum_lapsed_business_minutes_new (the sum_lapsed_business_minutes from prior row)
  from filtered_reply_times

), reply_time_breach as ( 
  select  
    *,
    case when is_sla_breached
      then sla_breach_at -- If the SLA was breached then record that time as the breach 
      else coalesce(agent_reply_at, next_solved_at) -- If the SLA was not breached then record either the agent_reply_at or next_solve_at as the breach event time as it was achieved.
    end as sla_update_at,
    case when total_runtime_minutes < 0 -- agent has already replied to prior to this SLA schedule
        then 0 -- so don't add new minutes to the SLA
      when total_new_minutes > sum_lapsed_business_minutes -- if total runtime, regardless of when the SLA schedule ended, is more than the total lapsed business minutes, that means the agent replied after the SLA schedule
          then sum_lapsed_business_minutes -- the elapsed time after the SLA end time should not be calculated as part of the business minutes, therefore sla_elapsed_time should only be sum_lapsed_business_minutes
      else sum_lapsed_business_minutes_new + ({{ dbt.datediff("sla_schedule_start_at", "coalesce(agent_reply_at, next_solved_at, current_time_check)", 'second') }} / 60) -- otherwise, the sla_elapsed_time will be sum_lapsed_business_minutes_new (the prior record's sum_lapsed_business_minutes) plus the minutes between SLA schedule start and agent_reply_time. If the agent hasn't replied yet, then the minute counter is still running, hence the coalesce of agent_reply_time and current_time_check.
    end as sla_elapsed_time
  from reply_time_breached_at_remove_old_sla 
)

select *
from reply_time_breach