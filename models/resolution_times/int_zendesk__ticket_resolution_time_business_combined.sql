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


), ticket_resolution_times as (


   select
       ticket_resolution_times_calendar.source_relation,
       ticket_resolution_times_calendar.ticket_id,
       ticket_schedules.schedule_created_at,
       ticket_schedules.schedule_invalidated_at,
       ticket_schedules.schedule_id,


       'first' as metric_type,


       -- bringing this in the determine which schedule (Daylight Savings vs Standard time) to use
       min(ticket_resolution_times_calendar.first_solved_at) as solved_at,


       ({{ dbt.datediff(
               "cast(" ~ zendesk.fivetran_week_start('ticket_schedules.schedule_created_at') ~ "as " ~ dbt.type_timestamp() ~ ")",
               "cast(ticket_schedules.schedule_created_at as " ~ dbt.type_timestamp() ~ ")",
               'second') }} /60
             ) as start_time_in_minutes_from_week,


       greatest(0,
         (
           {{ dbt.datediff(
             'ticket_schedules.schedule_created_at',
             'least(ticket_schedules.schedule_invalidated_at, min(ticket_resolution_times_calendar.first_solved_at))',
             'second') }}/60
         )) as raw_delta_in_minutes,


       {{ zendesk.fivetran_week_start('ticket_schedules.schedule_created_at') }} as start_week_date


   from ticket_resolution_times_calendar
   join ticket_schedules
     on ticket_resolution_times_calendar.ticket_id = ticket_schedules.ticket_id
    and ticket_resolution_times_calendar.source_relation = ticket_schedules.source_relation
   {{ dbt_utils.group_by(n=6) }}


   union all


   select
       ticket_resolution_times_calendar.source_relation,
       ticket_resolution_times_calendar.ticket_id,
       ticket_schedules.schedule_created_at,
       ticket_schedules.schedule_invalidated_at,
       ticket_schedules.schedule_id,


       'full' as metric_type,


       -- bringing this in the determine which schedule (Daylight Savings vs Standard time) to use
       min(ticket_resolution_times_calendar.last_solved_at) as solved_at,


       ({{ dbt.datediff(
               "cast(" ~ zendesk.fivetran_week_start('ticket_schedules.schedule_created_at') ~ "as " ~ dbt.type_timestamp() ~ ")",
               "cast(ticket_schedules.schedule_created_at as " ~ dbt.type_timestamp() ~ ")",
               'second') }} /60
             ) as start_time_in_minutes_from_week,


       greatest(0,
         (
           {{ dbt.datediff(
             'ticket_schedules.schedule_created_at',
             'least(ticket_schedules.schedule_invalidated_at, min(ticket_resolution_times_calendar.last_solved_at))',
             'second') }}/60
         )) as raw_delta_in_minutes,


       {{ zendesk.fivetran_week_start('ticket_schedules.schedule_created_at') }} as start_week_date


   from ticket_resolution_times_calendar
   join ticket_schedules
     on ticket_resolution_times_calendar.ticket_id = ticket_schedules.ticket_id
    and ticket_resolution_times_calendar.source_relation = ticket_schedules.source_relation
   {{ dbt_utils.group_by(n=6) }}


), weeks as (


   {{ dbt_utils.generate_series(var('max_ticket_length_weeks', 52)) }}


), weeks_cross_ticket_resolution_time as (


   -- because time is reported in minutes since the beginning of the week, we have to split up time spent on the ticket into calendar weeks
   select


     ticket_resolution_times.*,
     cast(generated_number - 1 as {{ dbt.type_int() }}) as week_number


   from ticket_resolution_times
   cross join weeks
   where floor((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)) >= generated_number - 1


), weekly_periods as (


   select


     weeks_cross_ticket_resolution_time.*,
     greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
     least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time


   from weeks_cross_ticket_resolution_time


), intercepted_periods as (


   select
     weekly_periods.source_relation,
     weekly_periods.ticket_id,
     weekly_periods.metric_type,
     weekly_periods.week_number,
     weekly_periods.schedule_id,
     weekly_periods.ticket_week_start_time,
     weekly_periods.ticket_week_end_time,
     schedule.start_time_utc as schedule_start_time,
     schedule.end_time_utc as schedule_end_time,
     least(weekly_periods.ticket_week_end_time, schedule.end_time_utc) - greatest(weekly_periods.ticket_week_start_time, schedule.start_time_utc) as scheduled_minutes
   from weekly_periods
   join schedule
     on weekly_periods.ticket_week_start_time <= schedule.end_time_utc
    and weekly_periods.ticket_week_end_time >= schedule.start_time_utc
    and weekly_periods.schedule_id = schedule.schedule_id
    and weekly_periods.source_relation = schedule.source_relation
    -- this chooses the Daylight Savings Time or Standard Time version of the schedule
    -- We have everything calculated within a week, so take us to the appropriate week first by adding the week_number * minutes-in-a-week to the minute-mark where we start and stop counting for the week
    and cast( {{ dbt.dateadd(datepart='minute', interval='cast(week_number * (7*24*60) + ticket_week_end_time as ' ~ dbt.type_int() ~ ")", from_date_or_timestamp='start_week_date') }} as date) > cast(schedule.valid_from as date)
    and cast( {{ dbt.dateadd(datepart='minute', interval='cast(week_number * (7*24*60) + ticket_week_start_time as ' ~ dbt.type_int() ~ ")", from_date_or_timestamp='start_week_date') }} as date) < cast(schedule.valid_until as date)


), ticket_resolution_business_minutes as (


   select
     source_relation,
     ticket_id,
     metric_type,
     sum(scheduled_minutes) as resolution_business_minutes
   from intercepted_periods
   group by 1, 2, 3


)


select
 ticket_resolution_business_minutes.source_relation,
 ticket_resolution_business_minutes.ticket_id,
 max(
   case
     when ticket_resolution_business_minutes.metric_type = 'first'
     then ticket_resolution_business_minutes.resolution_business_minutes
     else null
   end
 ) as first_resolution_business_minutes,


 max(
   case
     when ticket_resolution_business_minutes.metric_type = 'full'
     then ticket_resolution_business_minutes.resolution_business_minutes
     else null
   end
 ) as full_resolution_business_minutes


from ticket_resolution_business_minutes
group by 1, 2