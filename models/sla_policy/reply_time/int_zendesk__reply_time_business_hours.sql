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

), reply_time as (

  select *
  from {{ ref('int_zendesk__commenter_reply_at') }}

), ticket_updates as (

  select *
  from {{ ref('int_zendesk__updates') }}

), ticket_solved_times as (
  select
    source_relation,
    ticket_id,
    valid_starting_at as solved_at
  from ticket_updates
  where field_name = 'status'
  and value in ('solved','closed')

), schedule_business_hours as (

  select 
    source_relation,
    schedule_id,
    sum(end_time - start_time) as total_schedule_weekly_business_minutes
  -- referring to stg_zendesk__schedule instead of int_zendesk__schedule_spine just to calculate total minutes
  from {{ ref('stg_zendesk__schedule') }}
  group by 1, 2

), ticket_sla_applied_with_schedules as (
  -- Join to ALL schedule periods that overlap with the SLA period, not just the one
  -- active at sla_applied_at. This handles tickets that start under a 24/7 default
  -- schedule and are later routed to a business-hours schedule via a trigger.
  select
    sla_policy_applied.*,
    ticket_schedules.schedule_id,
    ticket_schedules.schedule_invalidated_at,
    greatest(sla_policy_applied.sla_applied_at, ticket_schedules.schedule_created_at) as schedule_period_start,
    schedule_business_hours.total_schedule_weekly_business_minutes

  from sla_policy_applied
  left join ticket_schedules on sla_policy_applied.ticket_id = ticket_schedules.ticket_id
    and sla_policy_applied.source_relation = ticket_schedules.source_relation
    and ticket_schedules.schedule_invalidated_at > sla_policy_applied.sla_applied_at
  left join schedule_business_hours
    on ticket_schedules.schedule_id = schedule_business_hours.schedule_id
    and ticket_schedules.source_relation = schedule_business_hours.source_relation
  where sla_policy_applied.in_business_hours
    and metric in ('next_reply_time', 'first_reply_time')

), ticket_sla_applied_with_schedule_week_info as (
  -- Compute week-relative start fields from schedule_period_start so the
  -- fivetran_week_start macro receives a simple column reference, not an expression.
  select
    *,
    ({{ dbt.datediff(
            "cast(" ~ zendesk.fivetran_week_start('sla_applied_at') ~ "as " ~ dbt.type_timestamp() ~ ")",
            "cast(schedule_period_start as " ~ dbt.type_timestamp() ~ ")",
            'second') }} /60
          ) as start_time_in_minutes_from_week,
    {{ zendesk.fivetran_week_start('sla_applied_at') }} as start_week_date
  from ticket_sla_applied_with_schedules

), first_reply_solve_times as (
  select
    ticket_sla_applied_with_schedule_week_info.source_relation,
    ticket_sla_applied_with_schedule_week_info.ticket_id,
    ticket_sla_applied_with_schedule_week_info.ticket_created_at,
    ticket_sla_applied_with_schedule_week_info.valid_starting_at,
    ticket_sla_applied_with_schedule_week_info.ticket_current_status,
    ticket_sla_applied_with_schedule_week_info.metric,
    ticket_sla_applied_with_schedule_week_info.latest_sla,
    ticket_sla_applied_with_schedule_week_info.sla_applied_at,
    ticket_sla_applied_with_schedule_week_info.target,
    ticket_sla_applied_with_schedule_week_info.in_business_hours,
    ticket_sla_applied_with_schedule_week_info.priority_applied,
    ticket_sla_applied_with_schedule_week_info.sla_policy_name,
    ticket_sla_applied_with_schedule_week_info.schedule_id,
    ticket_sla_applied_with_schedule_week_info.schedule_invalidated_at,
    ticket_sla_applied_with_schedule_week_info.schedule_period_start,
    ticket_sla_applied_with_schedule_week_info.start_time_in_minutes_from_week,
    ticket_sla_applied_with_schedule_week_info.total_schedule_weekly_business_minutes,
    ticket_sla_applied_with_schedule_week_info.start_week_date,
    min(reply_time.reply_at) as first_reply_time,
    min(ticket_solved_times.solved_at) as first_solved_time
  from ticket_sla_applied_with_schedule_week_info
  left join reply_time
    on reply_time.ticket_id = ticket_sla_applied_with_schedule_week_info.ticket_id
    and reply_time.reply_at > ticket_sla_applied_with_schedule_week_info.sla_applied_at
    and reply_time.source_relation = ticket_sla_applied_with_schedule_week_info.source_relation
  left join ticket_solved_times
    on ticket_sla_applied_with_schedule_week_info.ticket_id = ticket_solved_times.ticket_id
    and ticket_solved_times.solved_at > ticket_sla_applied_with_schedule_week_info.sla_applied_at
    and ticket_solved_times.source_relation = ticket_sla_applied_with_schedule_week_info.source_relation
  {{ dbt_utils.group_by(n=18) }}

), week_index_calc as (
    select
        *,
        {{ dbt.datediff("sla_applied_at", "least(coalesce(first_reply_time, " ~ dbt.current_timestamp() ~ "), coalesce(first_solved_time, " ~ dbt.current_timestamp() ~ "))", "week") }} + 1 as week_index,
        -- Minutes this schedule period contributes, capped at when the period ends.
        -- Do NOT clip at first_reply_time here — that causes rows to drop from intercepted_periods
        -- when the reply falls before any business hours window opens in that week.
        greatest(0, {{ dbt.datediff(
            "cast(schedule_period_start as " ~ dbt.type_timestamp() ~ ")",
            "cast(schedule_invalidated_at as " ~ dbt.type_timestamp() ~ ")",
            'second') }} / 60) as raw_delta_in_minutes
    from first_reply_solve_times

), weeks as (

    {{ dbt_utils.generate_series(var('max_ticket_length_weeks', 52)) }}

), weeks_cross_ticket_sla_applied as (
    -- because time is reported in minutes since the beginning of the week, we have to split up time spent on the ticket into calendar weeks
    select
      week_index_calc.*,
      cast(weeks.generated_number - 1 as {{ dbt.type_int() }}) as week_number

    from week_index_calc
    cross join weeks
    where week_index >= generated_number - 1
      -- also filter at the schedule-period level so short-lived periods don't generate
      -- weeks they don't extend into (which would produce negative ticket_week_end_time)
      and (start_time_in_minutes_from_week + raw_delta_in_minutes) > (cast(weeks.generated_number as {{ dbt.type_int() }}) - 1) * (7*24*60)

), weekly_periods as (
  
  select 
    weeks_cross_ticket_sla_applied.*,
    greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
    least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), 7*24*60) as ticket_week_end_time
  from weeks_cross_ticket_sla_applied

), intercepted_periods as (

  select 
    weekly_periods.*,
    schedule.start_time_utc as schedule_start_time,
    schedule.end_time_utc as schedule_end_time,
    (least(ticket_week_end_time, schedule.end_time_utc) - greatest(ticket_week_start_time,schedule.start_time_utc)) as lapsed_business_minutes,
    sum(least(ticket_week_end_time, schedule.end_time_utc) - greatest(ticket_week_start_time,schedule.start_time_utc)) over 
      (partition by ticket_id, sla_policy_name, metric, sla_applied_at {{ fivetran_utils.partition_by_source_relation(package_name='zendesk', alias='weekly_periods') }} 
        order by week_number, schedule.start_time_utc
        rows between unbounded preceding and current row) as sum_lapsed_business_minutes
  from weekly_periods
  join schedule on ticket_week_start_time <= schedule.end_time_utc 
    and ticket_week_end_time >= schedule.start_time_utc
    and weekly_periods.schedule_id = schedule.schedule_id
    and weekly_periods.source_relation = schedule.source_relation
    -- this chooses the Daylight Savings Time or Standard Time version of the schedule
    -- We have everything calculated within a week, so take us to the appropriate week first by adding the week_number * minutes-in-a-week to the minute-mark where we start and stop counting for the week
    and cast ({{ dbt.dateadd(datepart='minute', interval='cast(week_number * (7*24*60) + ticket_week_end_time as ' ~ dbt.type_int() ~ ")", from_date_or_timestamp='start_week_date') }} as date) > cast(schedule.valid_from as date)
    and cast ({{ dbt.dateadd(datepart='minute', interval='cast(week_number * (7*24*60) + ticket_week_start_time as ' ~ dbt.type_int() ~ ")", from_date_or_timestamp='start_week_date') }} as date) < cast(schedule.valid_until as date)

), intercepted_periods_with_breach_flag as (
  
  select 
    *,
    target - sum_lapsed_business_minutes as remaining_minutes,
    case when (target - sum_lapsed_business_minutes) < 0 
      and 
        (lag(target - sum_lapsed_business_minutes) over
        (partition by ticket_id, sla_policy_name, metric, sla_applied_at {{ fivetran_utils.partition_by_source_relation(package_name='zendesk') }} order by week_number, schedule_start_time) >= 0 
        or 
        lag(target - sum_lapsed_business_minutes) over
        (partition by ticket_id, sla_policy_name, metric, sla_applied_at {{ fivetran_utils.partition_by_source_relation(package_name='zendesk') }} order by week_number, schedule_start_time) is null) 
        then true else false end as is_breached_during_schedule -- this flags the scheduled period on which the breach took place
  from intercepted_periods

), intercepted_periods_with_breach_flag_calculated as (

  select
    *,
    schedule_end_time + remaining_minutes as breached_at_minutes,
    {{ zendesk.fivetran_week_start('sla_applied_at') }} as starting_point,
    {{ fivetran_utils.timestamp_add(
        "second",
        "cast(((7*24*60*60) * week_number) + ((schedule_end_time + remaining_minutes) * 60) as " ~ dbt.type_int() ~ " )",
        "cast(" ~ zendesk.fivetran_week_start('sla_applied_at') ~ " as " ~ dbt.type_timestamp() ~ ")" ) }} as sla_breach_at,
    {{ fivetran_utils.timestamp_add(
        "second",
        "cast(((7*24*60*60) * week_number) + (schedule_start_time * 60) as " ~ dbt.type_int() ~ " )",
        "cast(" ~ zendesk.fivetran_week_start('sla_applied_at') ~ " as " ~ dbt.type_timestamp() ~ ")" ) }} as sla_schedule_start_at,
    least({{ fivetran_utils.timestamp_add(
        "second",
        "cast(((7*24*60*60) * week_number) + (schedule_end_time * 60) as " ~ dbt.type_int() ~ " )",
        "cast(" ~ zendesk.fivetran_week_start('sla_applied_at') ~ " as " ~ dbt.type_timestamp() ~ ")" ) }}, schedule_invalidated_at) as sla_schedule_end_at,
    {{ zendesk.fivetran_week_end("sla_applied_at") }} as week_end_date
  from intercepted_periods_with_breach_flag

), reply_time_business_hours_sla as (

  select
    source_relation,
    ticket_id,
    sla_policy_name,
    metric,
    ticket_created_at,
    sla_applied_at,
    greatest(schedule_period_start, sla_schedule_start_at) as sla_schedule_start_at,
    sla_schedule_end_at,
    target,
    sum_lapsed_business_minutes,
    in_business_hours,
    priority_applied,
    sla_breach_at,
    is_breached_during_schedule,
    total_schedule_weekly_business_minutes,
    max(case when is_breached_during_schedule then sla_breach_at else null end) over (partition by ticket_id, sla_policy_name, metric, sla_applied_at, target {{ fivetran_utils.partition_by_source_relation(package_name='zendesk') }}) as sla_breach_exact_time,
    week_number
  from intercepted_periods_with_breach_flag_calculated

) 

select * 
from reply_time_business_hours_sla