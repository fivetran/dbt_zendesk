
-- Calculate breach time for agent work time, calendar hours
with sla_policy_applied as (

  select *
  from {{ ref('sla_policy_applied') }}

), agent_work_time_calendar_sla as (

  select
    sla_policy_applied.*
  from sla_policy_applied 
  where sla_policy_applied.metric = 'agent_work_time'
    and sla_policy_applied.in_business_hours = 'false'
    
), ticket_agent_work_times_post_sla as (
  select  
    ticket_historical_status.ticket_id,
    greatest(ticket_historical_status.valid_starting_at, agent_work_time_calendar_sla.sla_applied_at) as valid_starting_at,
    ticket_historical_status.valid_ending_at,
    ticket_historical_status.status as ticket_status,
    agent_work_time_calendar_sla.metric,
    agent_work_time_calendar_sla.sla_applied_at,
    agent_work_time_calendar_sla.target,    
    agent_work_time_calendar_sla.ticket_created_at
  from ticket_historical_status
  join agent_work_time_calendar_sla
    on ticket_historical_status.ticket_id = agent_work_time_calendar_sla.ticket_id
  where status in ('new', 'open')
  and sla_applied_at < valid_ending_at

  -- might be able to combine the lines of code above with the start of the calendar hours breached details

), agent_work_time_calendar_minutes as (

  select 
    *,
    timestamp_diff(valid_ending_at, valid_starting_at, minute) as calendar_minutes,
    sum(timestamp_diff(valid_ending_at, valid_starting_at, minute)) 
      over (partition by ticket_id, sla_applied_at order by valid_starting_at) as running_total_calendar_minutes
  from ticket_agent_work_times_post_sla

), agent_work_time_calendar_minutes_flagged as (

select 
  agent_work_time_calendar_minutes.*,
  target - running_total_calendar_minutes as remaining_target_minutes,
  case when (target - running_total_calendar_minutes) < 0 
      and 
        (lag(target - running_total_calendar_minutes) over
        (partition by ticket_id, sla_applied_at order by valid_starting_at) >= 0 
        or 
        lag(target - running_total_calendar_minutes) over
        (partition by ticket_id, sla_applied_at order by valid_starting_at) is null) 
        then true else false end as is_breached_during_schedule
        
from  agent_work_time_calendar_minutes

), agent_work_calendar_breach as (

  select
    *,
    (remaining_target_minutes + calendar_minutes) as breach_minutes,
    timestamp_add(valid_starting_at, 
      interval (remaining_target_minutes + calendar_minutes) minute) as breached_at
  from agent_work_time_calendar_minutes_flagged
  where is_breached_during_schedule