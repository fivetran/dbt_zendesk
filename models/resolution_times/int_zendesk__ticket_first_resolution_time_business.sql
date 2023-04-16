{{ config(enabled=var('using_schedules', True)) }}

with ticket_resolution_times_calendar as (

    select *
    from {{ ref('int_zendesk__ticket_resolution_times_calendar') }}

), ticket_schedules as (

    select *
    from {{ ref('int_zendesk__ticket_schedules') }}

), schedule as (

    select *
    from {{ ref('int_zendesk__schedule_spine') }}

), ticket_first_resolution_time as (

  select 
    ticket_resolution_times_calendar.ticket_id,
    ticket_schedules.schedule_created_at,
    ticket_schedules.schedule_invalidated_at,
    ticket_schedules.schedule_id,

    -- bringing this in the determine which schedule (Daylight Savings vs Standard time) to use
    min(ticket_resolution_times_calendar.first_solved_at) as first_solved_at,

    ({{ dbt.datediff(
            "cast(" ~ dbt_date.week_start('ticket_schedules.schedule_created_at','UTC') ~ "as " ~ dbt.type_timestamp() ~ ")", 
            "cast(ticket_schedules.schedule_created_at as " ~ dbt.type_timestamp() ~ ")",
            'second') }} /60
          ) as start_time_in_minutes_from_week,
    greatest(0,
      (
        {{ dbt.datediff(
          'ticket_schedules.schedule_created_at',
          'least(ticket_schedules.schedule_invalidated_at, min(ticket_resolution_times_calendar.first_solved_at))',
          'second') }}/60
        )) as raw_delta_in_minutes
      
  from ticket_resolution_times_calendar
  join ticket_schedules on ticket_resolution_times_calendar.ticket_id = ticket_schedules.ticket_id
  group by 1, 2, 3, 4

), weeks as (

    {{ dbt_utils.generate_series(208) }}

), weeks_cross_ticket_first_resolution_time as (
    -- because time is reported in minutes since the beginning of the week, we have to split up time spent on the ticket into calendar weeks
    select 

      ticket_first_resolution_time.*,
      generated_number - 1 as week_number

    from ticket_first_resolution_time
    cross join weeks
    where floor((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)) >= generated_number - 1


), weekly_periods as (
  
    select 

      weeks_cross_ticket_first_resolution_time.*,
      greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
      least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time
    
    from weeks_cross_ticket_first_resolution_time

), intercepted_periods as (

  select ticket_id,
        week_number,
        weekly_periods.schedule_id,
        ticket_week_start_time,
        ticket_week_end_time,
        schedule.start_time_utc as schedule_start_time,
        schedule.end_time_utc as schedule_end_time,
        schedule.holiday_start_date_at,
        schedule.holiday_end_date_at,
        schedule.holiday_start_time_from_week,
        schedule.holiday_end_time_from_week,
        schedule.holiday_start_time_from_week_utc as holiday_start_time,
        schedule.holiday_end_time_from_week_utc as holiday_end_time,

        least(schedule.holiday_start_time_from_week_utc,ticket_week_end_time, schedule.end_time_utc) - greatest(schedule.holiday_end_time_from_week_utc, ticket_week_start_time, schedule.start_time_utc) as scheduled_minutes
        --  least(ticket_week_end_time, schedule.end_time_utc) - greatest(ticket_week_start_time, schedule.start_time_utc) as scheduled_minutes
  from weekly_periods
  join schedule on ticket_week_start_time <= schedule.end_time_utc 
    and ticket_week_end_time >= schedule.start_time_utc
    and weekly_periods.schedule_id = schedule.schedule_id
    -- this chooses the Daylight Savings Time or Standard Time version of the schedule
    and weekly_periods.first_solved_at >= cast(schedule.valid_from as {{ dbt.type_timestamp() }})
    and weekly_periods.first_solved_at < cast(schedule.valid_until as {{ dbt.type_timestamp() }}) 
    -- only join the row from schedule with the relevant holiday if any exist. This is because a given schedule can have more than 1 holiday, and we want to limit fanouts. Therefore we want to only include any holidays that may overlap with ticket times.
    and (ticket_week_start_time <= holiday_start_time_from_week_utc and ticket_week_start_time not >= holiday_end_time_from_week_utc)
    or (ticket_week_start_time >= holiday_start_time_from_week_utc and ticket_week_start_time not <= holiday_end_time_from_week_utc)

)

  select
    ticket_id,
    sum(scheduled_minutes) as first_resolution_business_minutes
  from intercepted_periods
  group by 1