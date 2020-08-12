{{ config(enabled=var('using_sla_policy', True)) }}

-- step 3, determine when an SLA will breach for SLAs that are in business hours

with ticket as (
  
  select *
  from {{ ref('stg_zendesk_ticket') }}

), ticket_schedule as (
 
  select *
  from {{ ref('stg_zendesk_ticket_schedule') }}

), schedule as (
 
  select 
    
    cast(schedule_id as string) as schedule_id,
    end_time_utc,
    start_time_utc,
    schedule_name,
    created_at

  from {{ ref('stg_zendesk_schedule') }}

), sla_policy_applied as (
 
  select *
  from {{ ref('sla_policy_applied') }}


), default_schedule_events as (
-- Goal: understand the working schedules applied to tickets, so that we can then determine the applicable business hours/schedule.
-- Your default schedule is used for all tickets, unless you set up a trigger to apply a specific schedule to specific tickets.

-- This portion of the query creates ticket_schedules for these "default" schedules, as the tikcet_schedule table only includes
-- trigger schedules

{% if execute %}

    {% set default_schedule_id_query %}
        with set_default_schedule_flag as (
          select 
            row_number() over (order by created_at) = 1 as is_default_schedule,
            schedule_id
          from {{ ref('stg_zendesk_schedule') }}
        )
        select 
          schedule_id
        from set_default_schedule_flag
        where is_default_schedule

    {% endset %}

    {% set default_schedule_id = run_query(default_schedule_id_query).columns[0][0]|string %}

    {% endif %}

  select
    ticket.ticket_id,
    ticket.created_at as schedule_created_at,
    '{{default_schedule_id}}' as schedule_id
  from ticket
  left join ticket_schedule as first_schedule
    on first_schedule.ticket_id = ticket.ticket_id
    and timestamp_add(first_schedule.created_at, interval -5 second) <= ticket.created_at -- make cross-db compatible
    and first_schedule.created_at >= ticket.created_at    
  where first_schedule.ticket_id is null

), schedule_events as (
  
  select
    *
  from default_schedule_events
  
  union all
  
  select 
    ticket_id,
    created_at as schedule_created_at,
    cast(schedule_id as string) as schedule_id
  from ticket_schedule

), ticket_schedules as (
  
  select 
    ticket_id,
    schedule_id,
    schedule_created_at,
    coalesce(lead(schedule_created_at) over (partition by ticket_id order by schedule_created_at)
            , timestamp("9999-12-31 01:01:01")) as schedule_invalidated_at --- make cross db compatible
  from schedule_events
      
-- -- step 3b, using the sla target and sla_applied_at, figure out when the breach will happen for sla's that are in business hours.

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
    and weekly_periods.schedule_id = cast(schedule.schedule_id as string)
  
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