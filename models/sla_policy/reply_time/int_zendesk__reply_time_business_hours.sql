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

), users as (

  select *
  from {{ ref('int_zendesk__user_aggregates') }}

), ticket_updates as (

  select *
  from {{ ref('int_zendesk__updates') }}

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

), schedule_business_hours as (

  select 
    schedule_id,
    sum(end_time - start_time) as total_schedule_weekly_business_minutes
  -- referring to stg_zendesk__schedule instead of int_zendesk__schedule_spine just to calculate total minutes
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
      schedule_business_hours.total_schedule_weekly_business_minutes,
    {{ dbt_date.week_start('sla_policy_applied.sla_applied_at','UTC') }} as start_week_date

  from sla_policy_applied
  left join ticket_schedules on sla_policy_applied.ticket_id = ticket_schedules.ticket_id
    and {{ fivetran_utils.timestamp_add('second', -1, 'ticket_schedules.schedule_created_at') }} <= sla_policy_applied.sla_applied_at
    and {{ fivetran_utils.timestamp_add('second', -1, 'ticket_schedules.schedule_invalidated_at') }} > sla_policy_applied.sla_applied_at
  left join schedule_business_hours 
    on ticket_schedules.schedule_id = schedule_business_hours.schedule_id
  where sla_policy_applied.in_business_hours
    and metric in ('next_reply_time', 'first_reply_time')

), first_reply_solve_times as (
  select
    ticket_sla_applied_with_schedules.ticket_id,
    ticket_sla_applied_with_schedules.ticket_created_at,
    ticket_sla_applied_with_schedules.valid_starting_at,
    ticket_sla_applied_with_schedules.ticket_current_status,
    ticket_sla_applied_with_schedules.metric,
    ticket_sla_applied_with_schedules.latest_sla,
    ticket_sla_applied_with_schedules.sla_applied_at,
    ticket_sla_applied_with_schedules.target,
    ticket_sla_applied_with_schedules.in_business_hours,
    ticket_sla_applied_with_schedules.sla_policy_name,
    ticket_sla_applied_with_schedules.schedule_id,
    ticket_sla_applied_with_schedules.start_time_in_minutes_from_week,
    ticket_sla_applied_with_schedules.total_schedule_weekly_business_minutes,
    ticket_sla_applied_with_schedules.start_week_date,
    min(reply_time.reply_at) as first_reply_time,
    min(ticket_solved_times.solved_at) as first_solved_time
  from ticket_sla_applied_with_schedules
  left join reply_time
    on reply_time.ticket_id = ticket_sla_applied_with_schedules.ticket_id
    and reply_time.reply_at > ticket_sla_applied_with_schedules.sla_applied_at
  left join ticket_solved_times
    on ticket_sla_applied_with_schedules.ticket_id = ticket_solved_times.ticket_id
    and ticket_solved_times.solved_at > ticket_sla_applied_with_schedules.sla_applied_at
  {{ dbt_utils.group_by(n=14) }}

), week_index_calc as (
    select 
        *,
        {{ dbt.datediff("sla_applied_at", "least(first_reply_time, first_solved_time)", "week") }} + 1 as week_index
    from first_reply_solve_times

), weeks as (

    {{ dbt_utils.generate_series(52) }}

), weeks_cross_ticket_sla_applied as (
    -- because time is reported in minutes since the beginning of the week, we have to split up time spent on the ticket into calendar weeks
    select
      week_index_calc.*,
      cast(weeks.generated_number - 1 as {{ dbt.type_int() }}) as week_number

    from week_index_calc
    cross join weeks
    where week_index >= generated_number - 1

), weekly_periods as (
  
  select 
    weeks_cross_ticket_sla_applied.*,
    cast(greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as {{ dbt.type_int() }}) as ticket_week_start_time,
    cast((7*24*60) as {{ dbt.type_int() }}) as ticket_week_end_time
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
    -- We have everything calculated within a week, so take us to the appropriate week first by adding the week_number * minutes-in-a-week to the minute-mark where we start and stop counting for the week
    and cast ({{ dbt.dateadd(datepart='minute', interval='week_number * (7*24*60) + ticket_week_end_time', from_date_or_timestamp='start_week_date') }} as date) > cast(schedule.valid_from as date)
    and cast ({{ dbt.dateadd(datepart='minute', interval='week_number * (7*24*60) + ticket_week_start_time', from_date_or_timestamp='start_week_date') }} as date) < cast(schedule.valid_until as date)

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
        "cast(" ~ dbt_date.week_start('sla_applied_at','UTC') ~ " as " ~ dbt.type_timestamp() ~ ")" ) }} as sla_breach_at,
    {{ fivetran_utils.timestamp_add(
        "minute",
        "cast(((7*24*60) * week_number) + (schedule_start_time) as " ~ dbt.type_int() ~ " )",
        "cast(" ~ dbt_date.week_start('sla_applied_at','UTC') ~ " as " ~ dbt.type_timestamp() ~ ")" ) }} as sla_schedule_start_at,
    {{ fivetran_utils.timestamp_add(
        "minute",
        "cast(((7*24*60) * week_number) + (schedule_end_time) as " ~ dbt.type_int() ~ " )",
        "cast(" ~ dbt_date.week_start('sla_applied_at','UTC') ~ " as " ~ dbt.type_timestamp() ~ ")" ) }} as sla_schedule_end_at,
    {{ dbt_date.week_end("sla_applied_at", tz="America/UTC") }} as week_end_date
  from intercepted_periods_with_breach_flag

), reply_time_business_hours_sla as (

  select
    ticket_id,
    sla_policy_name,
    metric,
    ticket_created_at,
    sla_applied_at,
    greatest(sla_applied_at,sla_schedule_start_at) as sla_schedule_start_at,
    sla_schedule_end_at,
    target,
    sum_lapsed_business_minutes,
    in_business_hours,
    sla_breach_at,
    is_breached_during_schedule,
    total_schedule_weekly_business_minutes,
    max(case when is_breached_during_schedule then sla_breach_at else null end) over (partition by ticket_id, metric, sla_applied_at, target) as sla_breach_exact_time,
    week_number
  from intercepted_periods_with_breach_flag_calculated

) 

select * 
from reply_time_business_hours_sla