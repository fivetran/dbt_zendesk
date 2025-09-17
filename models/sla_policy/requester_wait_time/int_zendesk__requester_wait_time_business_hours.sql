{{ config(enabled=var('using_schedules', True)) }}

-- REQUESTER WAIT TIME
-- This is complicated, as SLAs minutes are only counted while the ticket is in 'new', 'open', and 'on-hold' status.

-- Additionally, for business hours, only 'new', 'open', and 'on-hold' status hours are counted if they are also during business hours
with requester_wait_time_filtered_statuses as (

  select *
  from {{ ref('int_zendesk__requester_wait_time_filtered_statuses') }}
  where in_business_hours

), schedule as (

  select * 
  from {{ ref('int_zendesk__schedule_spine') }}

), ticket_schedules as (

  select * 
  from {{ ref('int_zendesk__ticket_schedules') }}
  
-- cross schedules with work time
), ticket_status_crossed_with_schedule as (
  
    select
      requester_wait_time_filtered_statuses.source_relation,
      requester_wait_time_filtered_statuses.ticket_id,
      requester_wait_time_filtered_statuses.sla_applied_at,
      requester_wait_time_filtered_statuses.target,
      requester_wait_time_filtered_statuses.sla_policy_name,
      ticket_schedules.schedule_id,

      -- take the intersection of the intervals in which the status and the schedule were both active, for calculating the business minutes spent working on the ticket
      greatest(valid_starting_at, schedule_created_at) as valid_starting_at,
      least(valid_ending_at, schedule_invalidated_at) as valid_ending_at,

      -- bringing the following in the determine which schedule (Daylight Savings vs Standard time) to use
      valid_starting_at as status_valid_starting_at,
      valid_ending_at as status_valid_ending_at

    from requester_wait_time_filtered_statuses
    left join ticket_schedules
      on requester_wait_time_filtered_statuses.ticket_id = ticket_schedules.ticket_id
      and requester_wait_time_filtered_statuses.source_relation = ticket_schedules.source_relation
    where {{ dbt.datediff(
              'greatest(valid_starting_at, schedule_created_at)', 
              'least(valid_ending_at, schedule_invalidated_at)', 
              'second') }} > 0

), ticket_full_solved_time as (

    select 
      source_relation,
      ticket_id,
      sla_applied_at,
      target,
      sla_policy_name,
      schedule_id,
      valid_starting_at,
      valid_ending_at,
      status_valid_starting_at,
      status_valid_ending_at,
      ({{ dbt.datediff(
            "cast(" ~ zendesk.fivetran_week_start('ticket_status_crossed_with_schedule.valid_starting_at') ~ "as " ~ dbt.type_timestamp() ~ ")", 
            "cast(ticket_status_crossed_with_schedule.valid_starting_at as " ~ dbt.type_timestamp() ~ ")",
            'second') }} /60
          ) as valid_starting_at_in_minutes_from_week,
      ({{ dbt.datediff(
              'ticket_status_crossed_with_schedule.valid_starting_at', 
              'ticket_status_crossed_with_schedule.valid_ending_at',
              'second') }} /60
            ) as raw_delta_in_minutes,
    {{ zendesk.fivetran_week_start('ticket_status_crossed_with_schedule.valid_starting_at') }} as start_week_date

    from ticket_status_crossed_with_schedule
    {{ dbt_utils.group_by(n=11) }}

), weeks as (

    {{ dbt_utils.generate_series(52) }}

), weeks_cross_ticket_full_solved_time as (
    -- because time is reported in minutes since the beginning of the week, we have to split up time spent on the ticket into calendar weeks
    select 
      ticket_full_solved_time.*,
      cast(generated_number - 1 as {{ dbt.type_int() }}) as week_number
    from ticket_full_solved_time
    cross join weeks
    where floor((valid_starting_at_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)) >= generated_number -1

), weekly_period_requester_wait_time as (

    select 
      source_relation,
      ticket_id,
      sla_applied_at,
      valid_starting_at,
      valid_ending_at,
      status_valid_starting_at,
      status_valid_ending_at,
      target,
      sla_policy_name,
      valid_starting_at_in_minutes_from_week,
      raw_delta_in_minutes,
      week_number,
      schedule_id,
      start_week_date,
      greatest(0, valid_starting_at_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time_minute,
      least(valid_starting_at_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time_minute
    
    from weeks_cross_ticket_full_solved_time

), intercepted_periods_agent as (
  
    select 
      weekly_period_requester_wait_time.source_relation,
      weekly_period_requester_wait_time.ticket_id,
      weekly_period_requester_wait_time.sla_applied_at,
      weekly_period_requester_wait_time.target,
      weekly_period_requester_wait_time.sla_policy_name,
      weekly_period_requester_wait_time.valid_starting_at,
      weekly_period_requester_wait_time.valid_ending_at,
      weekly_period_requester_wait_time.week_number,
      weekly_period_requester_wait_time.ticket_week_start_time_minute,
      weekly_period_requester_wait_time.ticket_week_end_time_minute,
      coalesce(schedule.start_time_utc, 0) as schedule_start_time,  -- fill 0 for schedules completely outside schedule window. Only necessary for this field for use downstream.
      schedule.end_time_utc as schedule_end_time,
      coalesce(
        least(ticket_week_end_time_minute, schedule.end_time_utc)
        - greatest(weekly_period_requester_wait_time.ticket_week_start_time_minute, schedule.start_time_utc),
        0) as scheduled_minutes --- fill 0 for schedules completely outside schedule window. Only necessary for this field for use downstream.
    from weekly_period_requester_wait_time
    left join schedule -- using a left join to account for tickets started and completed entirely outside of a schedule, otherwise they are filtered out
      on ticket_week_start_time_minute <= schedule.end_time_utc 
      and ticket_week_end_time_minute >= schedule.start_time_utc
      and weekly_period_requester_wait_time.schedule_id = schedule.schedule_id
      and weekly_period_requester_wait_time.source_relation = schedule.source_relation
      -- this chooses the Daylight Savings Time or Standard Time version of the schedule
      -- We have everything calculated within a week, so take us to the appropriate week first by adding the week_number * minutes-in-a-week to the minute-mark where we start and stop counting for the week
      and cast( {{ dbt.dateadd(datepart='minute', interval='cast(week_number * (7*24*60) + ticket_week_end_time_minute as ' ~ dbt.type_int() ~ ")", from_date_or_timestamp='start_week_date') }} as date) > cast(schedule.valid_from as date)
      and cast( {{ dbt.dateadd(datepart='minute', interval='cast(week_number * (7*24*60) + ticket_week_start_time_minute as ' ~ dbt.type_int() ~ ")", from_date_or_timestamp='start_week_date') }} as date) < cast(schedule.valid_until as date)

), intercepted_periods_with_running_total as (
  
    select 
      *,
      sum(scheduled_minutes) over 
        (partition by ticket_id, sla_applied_at {{ partition_by_source_relation() }} 
          order by valid_starting_at, week_number, schedule_end_time
          rows between unbounded preceding and current row)
        as running_total_scheduled_minutes

    from intercepted_periods_agent


), intercepted_periods_agent_with_breach_flag as (
  select 
    intercepted_periods_with_running_total.*,
    target - running_total_scheduled_minutes as remaining_target_minutes,
    lag(target - running_total_scheduled_minutes) over
          (partition by ticket_id, sla_applied_at {{ partition_by_source_relation() }} order by valid_starting_at, week_number, schedule_end_time) as lag_check,
    case when (target - running_total_scheduled_minutes) = 0 then true
      when (target - running_total_scheduled_minutes) < 0 
        and 
          (lag(target - running_total_scheduled_minutes) over
          (partition by ticket_id, sla_applied_at {{ partition_by_source_relation() }} order by valid_starting_at, week_number, schedule_end_time) > 0 
          or 
          lag(target - running_total_scheduled_minutes) over
          (partition by ticket_id, sla_applied_at {{ partition_by_source_relation() }} order by valid_starting_at, week_number, schedule_end_time) is null) 
          then true else false end as is_breached_during_schedule
          
  from  intercepted_periods_with_running_total

), intercepted_periods_agent_filtered as (

  select
    *,
    (remaining_target_minutes + scheduled_minutes) as breach_minutes,
    greatest(ticket_week_start_time_minute, schedule_start_time) + (remaining_target_minutes + scheduled_minutes) as breach_minutes_from_week
  from intercepted_periods_agent_with_breach_flag

), requester_wait_business_breach as (
  
  select 
    *,
    {{ fivetran_utils.timestamp_add(
      "minute",
      "cast(((7*24*60) * week_number) + breach_minutes_from_week as " ~ dbt.type_int() ~ " )",
      "cast(" ~ zendesk.fivetran_week_start('valid_starting_at') ~ " as " ~ dbt.type_timestamp() ~ " )"
      ) }} as sla_breach_at
  from intercepted_periods_agent_filtered

)

select * 
from requester_wait_business_breach