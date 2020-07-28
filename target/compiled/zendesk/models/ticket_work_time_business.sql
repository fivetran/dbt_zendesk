with  __dbt__CTE__stg_zendesk_ticket_field_history as (
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
),  __dbt__CTE__stg_zendesk_ticket as (
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
),ticket_historical_status as (

    select *
    from __dbt__CTE__ticket_historical_status

), ticket_schedule as (

    select *
    from __dbt__CTE__ticket_schedule

), schedule as (

    select *
    from __dbt__CTE__stg_zendesk_schedule

), ticket_status_crossed_with_schedule as (
  
    select
      ticket_historical_status.ticket_id,
      ticket_historical_status.status as ticket_status,
      ticket_schedule.schedule_id,
      greatest(valid_starting_at, schedule_created_at) as status_schedule_start,
      least(valid_ending_at, schedule_invalidated_at) as status_schedule_end
    from ticket_historical_status
    left join ticket_schedule
      on ticket_historical_status.ticket_id = ticket_schedule.ticket_id
    where timestamp_diff(least(valid_ending_at, schedule_invalidated_at), greatest(valid_starting_at, schedule_created_at), second) > 0

), ticket_full_solved_time as (

    select 
      ticket_status_crossed_with_schedule.*,
      round(timestamp_diff(
              ticket_status_crossed_with_schedule.status_schedule_start, 
              timestamp_trunc(
                  ticket_status_crossed_with_schedule.status_schedule_start, 
                  week), 
              second)/60,
            0) as start_time_in_minutes_from_week,
      round(timestamp_diff(
              ticket_status_crossed_with_schedule.status_schedule_end, 
              ticket_status_crossed_with_schedule.status_schedule_start, 
              second)/60,
            0) as raw_delta_in_minutes
    from ticket_status_crossed_with_schedule
    group by 1, 2, 3, 4, 5

), weekly_periods as (

    select ticket_id,
          start_time_in_minutes_from_week,
          raw_delta_in_minutes,
          week_number,
          schedule_id,
          ticket_status,
          greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
          least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time
    from ticket_full_solved_time,
        unnest(generate_array(0, floor((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)), 1)) as week_number

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
      sum(agent_wait_time_in_minutes) as agent_wait_time_in_minutes,
      sum(requester_wait_time_in_minutes) as requester_wait_time_in_minutes,
      sum(agent_work_time_in_minutes) as agent_work_time_in_minutes,
      sum(on_hold_time_in_minutes) as on_hold_time_in_minutes
    from business_minutes
    group by 1

    