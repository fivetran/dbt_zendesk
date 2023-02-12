{{ config(enabled=var('using_schedules', True)) }}

-- step 3, determine when an SLA will breach for SLAs that are in business hours

with ticket_schedules as (
 
  select *
  from {{ ref('int_zendesk__ticket_schedules') }}

), schedule as (
 
  select *
  from {{ ref('int_zendesk__schedule_spine') }}

), sla_policy_applied as (
 
  select *
  from {{ ref('int_zendesk__sla_policy_applied') }}


), schedule_business_hours as (

  select 
    schedule_id,
    sum(end_time - start_time) as total_schedule_weekly_business_minutes
  -- referrinng to stg_zendesk__schedule instead of int_zendesk__schedule_spine just to calculcate total minutes
  from {{ ref('stg_zendesk__schedule') }}
  group by 1

), ticket_sla_applied_with_schedules as (

  select 
    sla_policy_applied.*,
    ticket_schedules.schedule_id,
    ({{ dbt.datediff(
            "cast(" ~ dbt_date.week_start('sla_policy_applied.sla_applied_at','UTC') ~ "as " ~ dbt.type_timestamp() ~ ")", 
            "cast(sla_policy_applied.sla_applied_at as " ~ dbt.type_timestamp() ~ ")",
            'second') }} /60
          ) as start_time_in_minutes_from_week,
      schedule_business_hours.total_schedule_weekly_business_minutes
  from sla_policy_applied
  left join ticket_schedules on sla_policy_applied.ticket_id = ticket_schedules.ticket_id
    and {{ fivetran_utils.timestamp_add('second', -1, 'ticket_schedules.schedule_created_at') }} <= sla_policy_applied.sla_applied_at
    and {{ fivetran_utils.timestamp_add('second', -1, 'ticket_schedules.schedule_invalidated_at') }} > sla_policy_applied.sla_applied_at
  left join schedule_business_hours 
    on ticket_schedules.schedule_id = schedule_business_hours.schedule_id
  where sla_policy_applied.in_business_hours
    and metric in ('next_reply_time', 'first_reply_time')
  
), weeks as (

    {{ dbt_utils.generate_series(208) }}

), weeks_cross_ticket_sla_applied as (
    -- because time is reported in minutes since the beginning of the week, we have to split up time spent on the ticket into calendar weeks
    select 

      ticket_sla_applied_with_schedules.*,
      generated_number - 1 as week_number

    from ticket_sla_applied_with_schedules
    cross join weeks
    where {{ fivetran_utils.ceiling('target/total_schedule_weekly_business_minutes') }} >= generated_number - 1

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
    -- this chooses the Daylight Savings Time or Standard Time version of the schedule
    and weekly_periods.sla_applied_at >= cast(schedule.valid_from as {{ dbt.type_timestamp() }})
    and weekly_periods.sla_applied_at < cast(schedule.valid_until as {{ dbt.type_timestamp() }})
  
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
    {{ dbt.date_trunc('week', 'sla_applied_at') }} as starting_point,
    {{ fivetran_utils.timestamp_add(
        "minute",
        "cast(((7*24*60) * week_number) + (schedule_end_time + remaining_minutes) as " ~ dbt.type_int() ~ " )",
        "" ~ dbt.date_trunc('week', 'sla_applied_at') ~ "" ) }} as sla_breach_at
  from intercepted_periods_with_breach_flag

), reply_time_business_hours_sla as (

  select
    ticket_id,
    sla_policy_name,
    metric,
    sla_applied_at,
    target,
    in_business_hours,
    sla_breach_at,
    is_breached_during_schedule
  from intercepted_periods_with_breach_flag_calculated

) 

select * 
from reply_time_business_hours_sla