{{ config(enabled=enabled_vars(['using_sla_policy','using_schedules'])) }}


-- step 3, determine when an SLA will breach for SLAs that are in business hours

with ticket_schedules as (
 
  select *
  from {{ ref('ticket_schedules') }}

), schedule as (
 
  select *
  from {{ ref('stg_zendesk_schedule') }}

), sla_policy_applied as (
 
  select *
  from {{ ref('sla_policy_applied') }}


), schedule_business_hours as (
  
  select 
    cast(schedule_id as string) as schedule_id,
    sum(end_time_utc - start_time_utc) as total_schedule_weekly_business_minutes
  from schedule
  group by 1

), ticket_sla_applied_with_schedules as (

  select 
    sla_policy_applied.*,
    ticket_schedules.schedule_id,
    round(
      timestamp_diff(sla_policy_applied.sla_applied_at, 
        timestamp_trunc(sla_policy_applied.sla_applied_at, week), second)/60
      , 0) as start_time_in_minutes_from_week,
      schedule_business_hours.total_schedule_weekly_business_minutes
  from sla_policy_applied
  left join ticket_schedules on sla_policy_applied.ticket_id = ticket_schedules.ticket_id
    and timestamp_add(ticket_schedules.schedule_created_at, interval -1 second) <= sla_policy_applied.sla_applied_at --cross db compatibility
    and timestamp_add(ticket_schedules.schedule_invalidated_at, interval -1 second) > sla_policy_applied.sla_applied_at --cross db compatibility
  left join schedule_business_hours 
    on ticket_schedules.schedule_id = schedule_business_hours.schedule_id
  where sla_policy_applied.in_business_hours = 'true'
    and metric in ('next_reply_time', 'first_reply_time')
  
), weekly_periods as (
  
  select 
    ticket_sla_applied_with_schedules.*,
    week_number,
    greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
    (7*24*60) as ticket_week_end_time
  from ticket_sla_applied_with_schedules, 
    unnest(generate_array(0, ceiling(target/total_schedule_weekly_business_minutes), 1)) as week_number  --generate the number of possible weeks the SLA breach will take
    --- we might need to figure out a different way to do this for cross db compatibility as not every warehouse supports unnest

), intercepted_periods as (

  select 
    weekly_periods.*,
    schedule.start_time_utc as schedule_start_time,
    schedule.end_time_utc as schedule_end_time,
    (schedule.end_time_utc - greatest(ticket_week_start_time,schedule.start_time_utc)) as lapsed_business_minutes,
    sum(schedule.end_time_utc - greatest(ticket_week_start_time,schedule.start_time_utc)) over 
      (partition by ticket_id, metric, sla_applied_at order by week_number, schedule.start_time_utc) as sum_lapsed_business_minutes
  from weekly_periods
  join schedule on ticket_week_start_time <= schedule.end_time_utc 
    and ticket_week_end_time >= schedule.start_time_utc
    and weekly_periods.schedule_id = schedule_id
  
), intercepted_periods_with_breach_flag as (
  
  select 
    *,
    target - sum_lapsed_business_minutes as remaining_minutes,
    case when (target - sum_lapsed_business_minutes) < 0 
      and 
        (lag(target - sum_lapsed_business_minutes) over
        (partition by ticket_id, metric, sla_applied_at order by week_number, schedule_start_time) >= 0 
        or 
        lag(target - sum_lapsed_business_minutes) over
        (partition by ticket_id, metric, sla_applied_at order by week_number, schedule_start_time) is null) 
        then true else false end as is_breached_during_schedule -- this flags the scheduled period on which the breach took place
  from intercepted_periods

), intercepted_periods_with_breach_flag_calculated as (

  select
    *,
    schedule_end_time + remaining_minutes as breached_at_minutes,
    timestamp_trunc(sla_applied_at, week) as starting_point,
    timestamp_add(timestamp_trunc(sla_applied_at, week), interval cast(((7*24*60) * week_number) + (schedule_end_time + remaining_minutes) as int64) minute) as breached_at
  from intercepted_periods_with_breach_flag
  where is_breached_during_schedule

), reply_time_business_hours_breached as (

  select
    ticket_id,
    metric,
    sla_applied_at,
    target,
    in_business_hours,
    breached_at
  from intercepted_periods_with_breach_flag_calculated

) 

select * 
from reply_time_business_hours_breached