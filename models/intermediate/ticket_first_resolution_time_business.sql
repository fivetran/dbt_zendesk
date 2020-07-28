with ticket_resolution_times_calendar as (

    select *
    from {{ ref('ticket_resolution_times_calendar') }}

), ticket_schedule as (

    select *
    from {{ ref('ticket_schedule') }}

), schedule as (

    select *
    from {{ ref('stg_zendesk_schedule') }}

), ticket_first_resolution_time as (

  select 
    ticket_resolution_times_calendar.ticket_id,
    ticket_schedule.schedule_created_at,
    ticket_schedule.schedule_invalidated_at,
    ticket_schedule.schedule_id,
    round(
      timestamp_diff(ticket_schedule.schedule_created_at, 
        timestamp_trunc(ticket_schedule.schedule_created_at, week), second)/60
      , 0) as start_time_in_minutes_from_week,
    greatest(0,
      round(
        timestamp_diff(
          least(ticket_schedule.schedule_invalidated_at, min(ticket_resolution_times_calendar.first_solved_at))
        ,ticket_schedule.schedule_created_at, second)/60
      , 0)) as raw_delta_in_minutes
  from ticket_resolution_times_calendar
  join ticket_schedule on ticket_resolution_times_calendar.ticket_id = ticket_schedule.ticket_id
  group by 1, 2, 3, 4

), weekly_periods as (
  
  select ticket_id,
         start_time_in_minutes_from_week,
         raw_delta_in_minutes,
         week_number,
         schedule_id,
         greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
         least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time
  from ticket_first_resolution_time, 
  unnest(generate_array(0, floor((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)), 1)) as week_number

), intercepted_periods as (

  select ticket_id,
         week_number,
         weekly_periods.schedule_id,
         ticket_week_start_time,
         ticket_week_end_time,
         schedule.start_time_utc as schedule_start_time,
         schedule.end_time_utc as schedule_end_time,
         least(ticket_week_end_time, schedule.end_time_utc) - greatest(ticket_week_start_time, schedule.start_time_utc) as scheduled_minutes
  from weekly_periods
  join schedule on ticket_week_start_time <= schedule.end_time_utc 
    and ticket_week_end_time >= schedule.start_time_utc
    and weekly_periods.schedule_id = schedule.schedule_id

)

  select ticket_id,
         sum(scheduled_minutes) as first_resolution_business_minutes
  from intercepted_periods
  group by 1