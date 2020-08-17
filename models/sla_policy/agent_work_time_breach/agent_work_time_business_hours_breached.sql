{{ config(enabled=enabled_vars(['using_sla_policy','using_schedules'])) }}

-- AGENT WORK TIME
-- This is complicated, as SLAs minutes are only counted while the ticket is in 'new' or 'open' status.

-- Additionally, for business hours, only 'new' or 'open' status hours are counted if they are also during business hours
with sla_policy_applied as (

  select *
  from {{ ref('sla_policy_applied') }}

), ticket_field_history as (

  select * 
  from {{ ref('stg_zendesk_ticket_field_history') }}

), schedule as (

  select * 
  from {{ ref('stg_zendesk_schedule') }}

), agent_work_time_business_sla as (
  select
    *
  from sla_policy_applied 
  where metric = 'agent_work_time'
    and in_business_hours = 'true'

-- Figure out when the ticket was in 'new' and 'open'
), ticket_historical_status as (

  select
    ticket_id,
    valid_starting_at,
    coalesce(valid_ending_at, timestamp_add(current_timestamp, interval 30 day)) as valid_ending_at,
    value as status,
  from zendesk.ticket_field_history
  where field_name = 'status'

), ticket_agent_work_times as (

  select  
    ticket_historical_status.ticket_id,
    agent_work_time_business_sla.ticket_created_at,
    greatest(ticket_historical_status.valid_starting_at, agent_work_time_business_sla.sla_applied_at) as valid_starting_at,
    ticket_historical_status.valid_ending_at,
    agent_work_time_business_sla.sla_applied_at,
    agent_work_time_business_sla.target,    
  from ticket_historical_status
  join agent_work_time_business_sla
    on ticket_historical_status.ticket_id = agent_work_time_business_sla.ticket_id
  where status in ('new', 'open') -- these are the only statuses that count as "agent work time"
  and sla_applied_at < valid_ending_at

), schedule as (

    select
      schedule_id,
      start_time_utc,
      end_time_utc
    from schedule

-- cross schedules with work time
), ticket_status_crossed_with_schedule as (
  
    select
      ticket_agent_work_times.ticket_id,
      ticket_agent_work_times.sla_applied_at,
--       ticket_agent_work_times.ticket_created_at,
      ticket_agent_work_times.target,      
      ticket_schedule.schedule_id,
      greatest(valid_starting_at, schedule_created_at) as valid_starting_at,
      least(valid_ending_at, schedule_invalidated_at) as valid_ending_at
    from ticket_agent_work_times
    left join ticket_schedule
      on ticket_agent_work_times.ticket_id = ticket_schedule.ticket_id
    where timestamp_diff(least(valid_ending_at, schedule_invalidated_at), greatest(valid_starting_at, schedule_created_at), second) > 0


), ticket_full_solved_time as (

    select 
      ticket_status_crossed_with_schedule.*,
      round(timestamp_diff(
              ticket_status_crossed_with_schedule.valid_starting_at, 
              timestamp_trunc(
                  ticket_status_crossed_with_schedule.valid_starting_at, 
                  week), 
              second)/60,
            0) as valid_starting_at_in_minutes_from_week,
      round(timestamp_diff(
              ticket_status_crossed_with_schedule.valid_ending_at, 
              ticket_status_crossed_with_schedule.valid_starting_at, 
              second)/60,
            0) as raw_delta_in_minutes
    from ticket_status_crossed_with_schedule
    group by 1, 2, 3, 4, 5, 6, 7

), weekly_period_agent_work_time as (

    select 
      ticket_id,
      sla_applied_at,
      valid_starting_at,
      valid_ending_at,
      target,
      valid_starting_at_in_minutes_from_week,
      raw_delta_in_minutes,
      week_number,
      schedule_id,
      greatest(0, valid_starting_at_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time_minute,
      least(valid_starting_at_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time_minute
    from ticket_full_solved_time,
        unnest(generate_array(0, floor((valid_starting_at_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)), 1)) as week_number

), intercepted_periods_agent as (
  
    select 
      weekly_period_agent_work_time.ticket_id,
      weekly_period_agent_work_time.sla_applied_at,
      weekly_period_agent_work_time.target,
      weekly_period_agent_work_time.valid_starting_at,
      weekly_period_agent_work_time.valid_ending_at,
      weekly_period_agent_work_time.week_number,
      weekly_period_agent_work_time.ticket_week_start_time_minute,
      weekly_period_agent_work_time.ticket_week_end_time_minute,
      schedule.start_time_utc as schedule_start_time,
      schedule.end_time_utc as schedule_end_time,
      least(ticket_week_end_time_minute, schedule.end_time_utc) - greatest(weekly_period_agent_work_time.ticket_week_start_time_minute, schedule.start_time_utc) as scheduled_minutes,
    from weekly_period_agent_work_time
    join schedule on ticket_week_start_time_minute <= schedule.end_time_utc 
      and ticket_week_end_time_minute >= schedule.start_time_utc
      and weekly_period_agent_work_time.schedule_id = schedule.schedule_id

), intercepted_periods_with_running_total as (
  
    select 
      *,
      sum(scheduled_minutes) over 
        (partition by ticket_id, sla_applied_at order by valid_starting_at, week_number, schedule_end_time)
        as running_total_scheduled_minutes

    from intercepted_periods_agent

), intercepted_periods_agent_with_breach_flag as (
  select 
    intercepted_periods_with_running_total.*,
    target - running_total_scheduled_minutes as remaining_target_minutes,
    case when (target - running_total_scheduled_minutes) = 0 then true
       when (target - running_total_scheduled_minutes) < 0 
        and 
          (lag(target - running_total_scheduled_minutes) over
          (partition by ticket_id, sla_applied_at order by valid_starting_at, week_number, schedule_end_time) > 0 
          or 
          lag(target - running_total_scheduled_minutes) over
          (partition by ticket_id, sla_applied_at order by valid_starting_at, week_number, schedule_end_time) is null) 
          then true else false end as is_breached_during_schedule
          
  from  intercepted_periods_with_running_total

), intercepted_periods_agent_filtered as (

  select
    *,
    (remaining_target_minutes + scheduled_minutes) as breach_minutes,
    greatest(ticket_week_start_time_minute, schedule_start_time) + (remaining_target_minutes + scheduled_minutes) as breach_minutes_from_week
  from intercepted_periods_agent_with_breach_flag
  where is_breached_during_schedule
  
-- Now we have agent work time business hours breached_at timestamps. Only SLAs that have been breached will appear in this list, otherwise
-- would be filtered out in the above
), agent_work_business_breach as (
  
  select 
    *,
    timestamp_add(
      timestamp_trunc(valid_starting_at, week),
      interval cast(((7*24*60) * week_number) + breach_minutes_from_week as int64) minute) as breached_at
  from intercepted_periods_agent_filtered

) 

select * 
from agent_work_business_breach