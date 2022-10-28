with commenter as (

    select * from {{ ref('stg_zendesk__user') }}

), ticket_reply_times as (
  
    select * from {{ ref('int_zendesk__ticket_reply_times') }}

), ticket_schedules as (

    select * from {{ ref('int_zendesk__ticket_schedules') }}

), schedule as (

    select * from {{ ref('int_zendesk__schedule_spine') }}

), replies as (

  select ticket_reply_times.ticket_id
    , ticket_reply_times.end_user_comment_created_at
    , ticket_reply_times.agent_responded_at
    , ticket_reply_times.responding_agent_user_id
    , ticket_reply_times.reply_time_calendar_minutes
    , ticket_schedules.schedule_created_at
    , ticket_schedules.schedule_invalidated_at
    , ticket_schedules.schedule_id

    , ({{ fivetran_utils.timestamp_diff(
            "cast(" ~ dbt_date.week_start('ticket_reply_times.end_user_comment_created_at','UTC') ~ "as " ~ dbt_utils.type_timestamp() ~ ")", 
            "cast(ticket_reply_times.end_user_comment_created_at as " ~ dbt_utils.type_timestamp() ~ ")",
            'second') }} / 60.0
          ) as start_time_in_minutes_from_week
  
  from ticket_reply_times
  join ticket_schedules on ticket_reply_times.ticket_id = ticket_schedules.ticket_id
  group by 1,2,3,4,5,6,7,8

), weeks as (

    {{ dbt_utils.generate_series(208) }}

), weeks_cross_ticket_reply_times as (
    -- because time is reported in minutes since the beginning of the week, we have to split up time spent on the ticket into calendar weeks
    select replies.*
      , generated_number - 1 as week_number

    from replies
    cross join weeks
    where floor((start_time_in_minutes_from_week + reply_time_calendar_minutes) / (7*24*60)) >= generated_number - 1

), weekly_periods as (
  
    select weeks_cross_ticket_reply_times.*
      , greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time
      , least(start_time_in_minutes_from_week + reply_time_calendar_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time

    from weeks_cross_ticket_reply_times

), intercepted_periods as (

  select  weekly_periods.*
      , schedule.start_time_utc as schedule_start_time
      , schedule.end_time_utc as schedule_end_time
      , least(ticket_week_end_time, schedule.end_time_utc) - greatest(ticket_week_start_time, schedule.start_time_utc) as scheduled_minutes

  from weekly_periods
  join schedule on ticket_week_start_time <= schedule.end_time_utc 
    and ticket_week_end_time >= schedule.start_time_utc
    and weekly_periods.schedule_id = schedule.schedule_id
    -- this chooses the Daylight Savings Time or Standard Time version of the schedule
    and weekly_periods.agent_responded_at >= cast(schedule.valid_from as {{ dbt_utils.type_timestamp() }})
    and weekly_periods.agent_responded_at < cast(schedule.valid_until as {{ dbt_utils.type_timestamp() }}) 

), aggregated as (

  select ticket_id
    , end_user_comment_created_at
    , agent_responded_at
    , responding_agent_user_id
    , reply_time_calendar_minutes
    , sum(scheduled_minutes) as reply_time_business_minutes

  from intercepted_periods
  group by 1,2,3,4,5

)

  select aggregated.*
    , commenter.name as responding_agent_name
    , commenter.email as responding_agent_email
  from aggregated
  left join commenter
    on commenter.user_id = aggregated.responding_agent_user_id