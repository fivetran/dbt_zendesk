with  __dbt__CTE__stg_zendesk_ticket as (
with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`ticket`

), fields as (

    select

      id as ticket_id,
      _fivetran_synced,
      assignee_id,
      brand_id,
      created_at,
      description,
      due_at,
      group_id,
      is_public,
      organization_id,
      priority,
      recipient,
      requester_id,
      status,
      subject,
      submitter_id,
      ticket_form_id,
      type,
      updated_at,
      url,
      via_channel as created_channel,
      via_source_from_id as source_from_id,
      via_source_from_title as source_from_title,
      via_source_rel as source_rel,
      via_source_to_address as source_to_address,
      via_source_to_name as source_to_name

    from base

)

select *
from fields
),  __dbt__CTE__stg_zendesk_user as (
with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`user`

), fields as (

    select

      id as user_id,
      _fivetran_synced,
      created_at,
      email,
      name,
      organization_id,
      role,
      ticket_restriction,
      time_zone,
      active as is_active

    from base

)

select *
from fields
),  __dbt__CTE__tickets_enhanced as (
with ticket as (

    select *
    from __dbt__CTE__stg_zendesk_ticket

), user as (

    select *
    from __dbt__CTE__stg_zendesk_user

), joined as (

    select 

        ticket.*,
        requester.role as requester_role,
        requester.role = 'agent' as agent_created_ticket,
        requester.email as requester_email,
        submitter.role as submitter_role,
        submitter.email as submitter_email,

    
    from ticket

    join user as requester
        on requester.user_id = ticket.requester_id
    
    join user as submitter
        on submitter.user_id = ticket.submitter_id
)

select *
from joined
),  __dbt__CTE__stg_zendesk_ticket_field_history as (
with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`ticket_field_history`

), fields as (
    
    select
    
      ticket_id,
      field_name,
      updated as valid_starting_at,
      lead(updated) over (partition by ticket_id, field_name order by updated) as valid_ending_at,
      value,
      user_id

    from base
    order by 1,2,3

)

select *
from fields
),  __dbt__CTE__ticket_historical_status as (
with ticket_status_history as (

    select *
    from __dbt__CTE__stg_zendesk_ticket_field_history
    where field_name = 'status'

)

  select
  
    ticket_id,
    valid_starting_at,
    valid_ending_at,
    timestamp_diff(coalesce(valid_ending_at,current_timestamp()),valid_starting_at, minute) as status_duration_calendar_minutes,
    value as status,
    row_number() over (partition by ticket_id order by valid_starting_at) as ticket_status_counter,
    row_number() over (partition by ticket_id, value order by valid_starting_at) as unique_status_counter

  from ticket_status_history
),  __dbt__CTE__ticket_resolution_times_calendar as (
with historical_solved_status as (

    select *
    from __dbt__CTE__ticket_historical_status
    where status = 'solved'

), ticket as (

    select *
    from __dbt__CTE__stg_zendesk_ticket

), solved_times as (
  
  select
  
    ticket_id,
    min(valid_starting_at) as first_solved_at,
    max(valid_starting_at) as last_solved_at

  from historical_solved_status
  group by 1

)

  select

    ticket.ticket_id,
    ticket.created_at,
    solved_times.first_solved_at,
    solved_times.last_solved_at,
    timestamp_diff(solved_times.first_solved_at,ticket.created_at, minute) as first_resolution_calendar_minutes,
    timestamp_diff(solved_times.last_solved_at,ticket.created_at, minute) as final_resolution_calendar_minutes

  from ticket
  left join solved_times
    on solved_times.ticket_id = ticket.ticket_id
),  __dbt__CTE__stg_zendesk_ticket_schedule as (


with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`ticket_schedule`

), fields as (
    
    select

      ticket_id,
      created_at,
      schedule_id,
      
    from base

)

select *
from fields
),  __dbt__CTE__ticket_schedule as (
with ticket_schedule as (

  select *
  from __dbt__CTE__stg_zendesk_ticket_schedule

), ticket as (

  select *
  from __dbt__CTE__stg_zendesk_ticket

)

select 
  ticket.ticket_id,
  coalesce(ticket_schedule.schedule_id, 15574 ) as schedule_id,
  coalesce(ticket_schedule.created_at, ticket.created_at) as schedule_created_at,
  coalesce(lead(
                ticket_schedule.created_at) over (partition by ticket.ticket_id order by ticket_schedule.created_at)
          , timestamp("9999-12-31 01:01:01")) as schedule_invalidated_at
from ticket

left join ticket_schedule
  on ticket.ticket_id = ticket_schedule.ticket_id
),  __dbt__CTE__stg_zendesk_schedule as (


with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`schedule`

), fields as (
    
    select

      id as schedule_id,
      end_time_utc,
      start_time_utc,
      name as schedule_name
      
    from base
    where not _fivetran_deleted

)

select *
from fields
),  __dbt__CTE__ticket_first_resolution_time_business as (
with ticket_resolution_times_calendar as (

    select *
    from __dbt__CTE__ticket_resolution_times_calendar

), ticket_schedule as (

    select *
    from __dbt__CTE__ticket_schedule

), schedule as (

    select *
    from __dbt__CTE__stg_zendesk_schedule

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
),  __dbt__CTE__ticket_full_resolution_time_business as (
with ticket_resolution_times_calendar as (

    select *
    from __dbt__CTE__ticket_resolution_times_calendar

), ticket_schedule as (

    select *
    from __dbt__CTE__ticket_schedule

), schedule as (

    select *
    from __dbt__CTE__stg_zendesk_schedule

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
          least(ticket_schedule.schedule_invalidated_at, min(ticket_resolution_times_calendar.last_solved_at))
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
),tickets_enhanced as (

  select *
  from __dbt__CTE__tickets_enhanced

), ticket_resolution_times_calendar as (

  select *
  from __dbt__CTE__ticket_resolution_times_calendar


--if using schedules

), ticket_first_resolution_time_business as (

  select *
  from __dbt__CTE__ticket_first_resolution_time_business

), ticket_full_resolution_time_business as (

  select *
  from __dbt__CTE__ticket_full_resolution_time_business

),



select
  tickets_enhanced.*,
  ticket_resolution_times_calendar.first_solved_at,
  ticket_resolution_times_calendar.last_solved_at,
  ticket_resolution_times_calendar.first_resolution_calendar_minutes,
  ticket_resolution_times_calendar.final_resolution_calendar_minutes,


---if using schedules
  ticket_first_resolution_time_business.first_resolution_business_minutes,
  ticket_full_resolution_time_business.first_resolution_business_minutes,

from tickets_enhanced

left join ticket_resolution_times_calendar
  on tickets_enhanced.ticket_id = ticket_resolution_times_calendar.ticket_id

---if using schedules

left join ticket_first_resolution_time_business
  on ticket_first_resolution_time_business.ticket_id = tickets_enhanced.ticket_id

left join ticket_full_resolution_time_business
  on ticket_full_resolution_time_business.ticket_id = tickets_enhanced.ticket_id