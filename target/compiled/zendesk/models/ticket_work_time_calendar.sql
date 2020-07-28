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
),ticket_historical_status as (

    select *
    from __dbt__CTE__ticket_historical_status

), calendar_minutes as (
  
    select 
      ticket_id,
      status,
      case when status in ('pending') then status_duration_calendar_minutes
          else 0 end as agent_wait_time_in_minutes,
      case when status in ('new', 'open', 'hold') then status_duration_calendar_minutes
          else 0 end as requester_wait_time_in_minutes,
      case when status in ('new', 'open') then status_duration_calendar_minutes
          else 0 end as agent_work_time_in_minutes,
      case when status in ('hold') then status_duration_calendar_minutes
          else 0 end as on_hold_time_in_minutes
    from ticket_historical_status

)

select 
  ticket_id,
  sum(agent_wait_time_in_minutes) as agent_wait_time_in_minutes,
  sum(requester_wait_time_in_minutes) as requester_wait_time_in_minutes,
  sum(agent_work_time_in_minutes) as agent_work_time_in_minutes,
  sum(on_hold_time_in_minutes) as on_hold_time_in_minutes
from calendar_minutes
group by 1