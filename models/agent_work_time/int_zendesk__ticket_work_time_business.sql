{{ config(enabled=var('using_schedules', True)) }}

with ticket_historical_status as (

    select *
    from {{ ref('int_zendesk__ticket_historical_status') }}

), ticket_schedules as (

    select *
    from {{ ref('int_zendesk__ticket_schedules') }}

), schedule as (

    select *
    from {{ ref('int_zendesk__schedule_spine') }}

), ticket_status_crossed_with_schedule as (
  
    select
      ticket_historical_status.ticket_id,
      ticket_historical_status.status as ticket_status,
      ticket_schedules.schedule_id,

      -- take the intersection of the intervals in which the status and the schedule were both active, for calculating the business minutes spent working on the ticket
      greatest(valid_starting_at, schedule_created_at) as status_schedule_start,
      least(valid_ending_at, schedule_invalidated_at) as status_schedule_end,

      -- bringing the following in the determine which schedule (Daylight Savings vs Standard time) to use
      ticket_historical_status.valid_starting_at as status_valid_starting_at,
      ticket_historical_status.valid_ending_at as status_valid_ending_at

    from ticket_historical_status
    left join ticket_schedules
      on ticket_historical_status.ticket_id = ticket_schedules.ticket_id
      where {{ dbt.datediff('greatest(valid_starting_at, schedule_created_at)', 'least(valid_ending_at, schedule_invalidated_at)', 'second') }} > 0

), ticket_full_solved_time as (

    select 
      ticket_id,
      ticket_status,
      schedule_id,
      status_schedule_start,
      status_schedule_end,
      status_valid_starting_at,
      status_valid_ending_at,
    ({{ dbt.datediff(
            "cast(" ~ dbt_date.week_start('ticket_status_crossed_with_schedule.status_schedule_start','UTC') ~ "as " ~ dbt.type_timestamp() ~ ")", 
            "cast(ticket_status_crossed_with_schedule.status_schedule_start as " ~ dbt.type_timestamp() ~ ")",
            'second') }} /60
          ) as start_time_in_minutes_from_week,
      ({{ dbt.datediff(
              'ticket_status_crossed_with_schedule.status_schedule_start',
              'ticket_status_crossed_with_schedule.status_schedule_end',
              'second') }} /60
            ) as raw_delta_in_minutes
    from ticket_status_crossed_with_schedule
    {{ dbt_utils.group_by(n=7) }}

), weeks as (

    {{ dbt_utils.generate_series(208) }}

), weeks_cross_ticket_full_solved_time as (
    -- because time is reported in minutes since the beginning of the week, we have to split up time spent on the ticket into calendar weeks
    select 
      ticket_full_solved_time.*,
      generated_number - 1 as week_number
    from ticket_full_solved_time
    cross join weeks
    where floor((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)) >= generated_number -1

), weekly_periods as (

    select

      weeks_cross_ticket_full_solved_time.*,
      greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
      least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time
    
    from weeks_cross_ticket_full_solved_time

), intercepted_periods as (
  
    select 
      weekly_periods.ticket_id,
      weekly_periods.week_number,
      weekly_periods.schedule_id,
      weekly_periods.ticket_status,
      weekly_periods.ticket_week_start_time,
      weekly_periods.ticket_week_end_time,
      schedule.start_time_utc as schedule_start_time,
      schedule.end_time_utc as schedule_end_time,
      least(ticket_week_end_time, schedule.end_time_utc) - greatest(weekly_periods.ticket_week_start_time, schedule.start_time_utc) as scheduled_minutes
    from weekly_periods
    join schedule on ticket_week_start_time <= schedule.end_time_utc 
      and ticket_week_end_time >= schedule.start_time_utc
      and weekly_periods.schedule_id = schedule.schedule_id
      -- this chooses the Daylight Savings Time or Standard Time version of the schedule
      and weekly_periods.status_valid_ending_at >= cast(schedule.valid_from as {{ dbt.type_timestamp() }})
      and weekly_periods.status_valid_starting_at < cast(schedule.valid_until as {{ dbt.type_timestamp() }}) 
  
), business_minutes as (
  
    select 
      ticket_id,
      ticket_status,
      case when ticket_status in ('pending') then scheduled_minutes
          else 0 end as agent_wait_time_in_minutes,
      case when ticket_status in ('new', 'open', 'hold') then scheduled_minutes
          else 0 end as requester_wait_time_in_minutes,
      case when ticket_status in ('new', 'open') then scheduled_minutes
          else 0 end as agent_work_time_in_minutes,
      case when ticket_status in ('hold') then scheduled_minutes
          else 0 end as on_hold_time_in_minutes
    from intercepted_periods

)
  
    select 
      ticket_id,
      sum(agent_wait_time_in_minutes) as agent_wait_time_in_business_minutes,
      sum(requester_wait_time_in_minutes) as requester_wait_time_in_business_minutes,
      sum(agent_work_time_in_minutes) as agent_work_time_in_business_minutes,
      sum(on_hold_time_in_minutes) as on_hold_time_in_business_minutes
    from business_minutes
    group by 1
