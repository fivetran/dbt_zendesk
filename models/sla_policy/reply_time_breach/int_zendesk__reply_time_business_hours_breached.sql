{{ config(enabled=fivetran_utils.enabled_vars(['using_sla_policy','using_schedules'])) }}


-- step 3, determine when an SLA will breach for SLAs that are in business hours

with ticket_schedules as (
 
  select *
  from {{ ref('int_zendesk__ticket_schedules') }}

), schedule as (
 
  select *
  from {{ ref('stg_zendesk__schedule') }}

), sla_policy_applied as (
 
  select *
  from {{ ref('int_zendesk__sla_policy_applied') }}


), schedule_business_hours as (
  
  select 
    schedule_id,
    sum(end_time_utc - start_time_utc) as total_schedule_weekly_business_minutes
  from schedule
  group by 1

), ticket_sla_applied_with_schedules as (

  select 
    sla_policy_applied.*,
    ticket_schedules.schedule_id,
    round(
      {{ fivetran_utils.timestamp_diff(
        "" ~ dbt_utils.date_trunc('week', 'sla_policy_applied.sla_applied_at') ~ "",
        'sla_policy_applied.sla_applied_at', 
        'second') }}/60
      , 0) as start_time_in_minutes_from_week,
      schedule_business_hours.total_schedule_weekly_business_minutes
  from sla_policy_applied
  left join ticket_schedules on sla_policy_applied.ticket_id = ticket_schedules.ticket_id
    and {{ fivetran_utils.timestamp_add('second', -1, 'ticket_schedules.schedule_created_at') }} <= sla_policy_applied.sla_applied_at
    and {{ fivetran_utils.timestamp_add('second', -1, 'ticket_schedules.schedule_invalidated_at') }} > sla_policy_applied.sla_applied_at
  left join schedule_business_hours 
    on ticket_schedules.schedule_id = schedule_business_hours.schedule_id
  where sla_policy_applied.in_business_hours = 'true'
    and metric in ('next_reply_time', 'first_reply_time')
  
), weeks as (

    {{ dbt_utils.generate_series(208) }}

), weeks_cross_ticket_sla_applied as (

    select 

      ticket_sla_applied_with_schedules.*,
      generated_number - 1 as week_number

    from ticket_sla_applied_with_schedules
    cross join weeks
    where {{ ceiling('target/total_schedule_weekly_business_minutes') }} >= generated_number - 1

), weekly_periods as (
  
  select 
    weeks_cross_ticket_sla_applied.*,
    greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
    (7*24*60) as ticket_week_end_time
  from weeks_cross_ticket_sla_applied

), intercepted_periods as (

  select 
    weekly_periods.*,
    schedule.start_time_utc as schedule_start_time,
    schedule.end_time_utc as schedule_end_time,
    (schedule.end_time_utc - greatest(ticket_week_start_time,schedule.start_time_utc)) as lapsed_business_minutes,
    sum(schedule.end_time_utc - greatest(ticket_week_start_time,schedule.start_time_utc)) over 
      (partition by ticket_id, metric, sla_applied_at 
        order by week_number, schedule.start_time_utc
        rows between unbounded preceding and current row) as sum_lapsed_business_minutes
  from weekly_periods
  join schedule on ticket_week_start_time <= schedule.end_time_utc 
    and ticket_week_end_time >= schedule.start_time_utc
    and weekly_periods.schedule_id = schedule.schedule_id
  
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
    {{ dbt_utils.date_trunc('week', 'sla_applied_at') }} as starting_point,
    {{ fivetran_utils.timestamp_add(
        "minute",
        "cast(((7*24*60) * week_number) + (schedule_end_time + remaining_minutes) as " ~ dbt_utils.type_int() ~ " )",
        "" ~ dbt_utils.date_trunc('week', 'sla_applied_at') ~ "" ) }} as breached_at
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