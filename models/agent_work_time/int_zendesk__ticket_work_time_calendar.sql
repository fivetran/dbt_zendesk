with ticket_historical_status as (

    select *
    from {{ ref('int_zendesk__ticket_historical_status') }}

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
  sum(agent_wait_time_in_minutes) as agent_wait_time_in_calendar_minutes,
  sum(requester_wait_time_in_minutes) as requester_wait_time_in_calendar_minutes,
  sum(agent_work_time_in_minutes) as agent_work_time_in_calendar_minutes,
  sum(on_hold_time_in_minutes) as on_hold_time_in_calendar_minutes
from calendar_minutes
group by 1