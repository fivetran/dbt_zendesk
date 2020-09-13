{{ config(enabled=var('using_schedules', True)) }}

with ticket_reply_times as (

    select *
    from {{ ref('ticket_reply_times') }}

), ticket_schedules as (

    select *
    from {{ ref('ticket_schedules') }}

), schedule as (

    select *
    from {{ ref('stg_zendesk_schedule') }}

), first_reply_time as (

    select
      ticket_id,
      end_user_comment_created_at,
      agent_responded_at

    from ticket_reply_times
    where is_first_comment

), ticket_first_reply_time as (

  select 
    first_reply_time.ticket_id,
    ticket_schedules.schedule_created_at,
    ticket_schedules.schedule_invalidated_at,
    ticket_schedules.schedule_id,
    round({{ timestamp_diff(
            "" ~ dbt_utils.date_trunc('week', 'ticket_schedules.schedule_created_at') ~ "", 
            'ticket_schedules.schedule_created_at',
            'second') }} /60,
          0) as start_time_in_minutes_from_week,
    greatest(0,
      round(
        {{ timestamp_diff(
          'ticket_schedules.schedule_created_at',
          'least(ticket_schedules.schedule_invalidated_at, min(first_reply_time.agent_responded_at))',
          'second') }}/60
      , 0)) as raw_delta_in_minutes
  
  from first_reply_time
  join ticket_schedules on first_reply_time.ticket_id = ticket_schedules.ticket_id
  group by 1, 2, 3, 4

), weeks as (

    {{ dbt_utils.generate_series(208) }}

), weeks_cross_ticket_first_reply as (

    select 

      ticket_first_reply_time.*,
      generated_number - 1 as week_number

    from ticket_first_reply_time
    cross join weeks
    where floor((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)) >= generated_number - 1

), weekly_periods as (
  
    select 
      weeks_cross_ticket_first_reply.*, 
      greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
      least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time
    from weeks_cross_ticket_first_reply

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
         sum(scheduled_minutes) as first_reply_time_business_minutes
  from intercepted_periods
  group by 1