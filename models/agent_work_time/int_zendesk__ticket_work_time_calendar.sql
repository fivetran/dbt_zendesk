with ticket_historical_status as (

    select *
    from {{ ref('int_zendesk__ticket_historical_status') }}

), calendar_minutes as (
  
    select 
        ticket_id,
        source_relation,
        status,
        case when status in ('pending') then status_duration_calendar_minutes
            else 0 end as agent_wait_time_in_minutes,
        case when status in ('new', 'open', 'hold') then status_duration_calendar_minutes
            else 0 end as requester_wait_time_in_minutes,
        case when status in ('new', 'open', 'hold', 'pending') then status_duration_calendar_minutes 
            else 0 end as solve_time_in_minutes, 
        case when status in ('new', 'open') then status_duration_calendar_minutes
            else 0 end as agent_work_time_in_minutes,
        case when status in ('hold') then status_duration_calendar_minutes
            else 0 end as on_hold_time_in_minutes,
        case when status = 'new' then status_duration_calendar_minutes
            else 0 end as new_status_duration_minutes,
        case when status = 'open' then status_duration_calendar_minutes
            else 0 end as open_status_duration_minutes,
        case when status = 'deleted' then 1
            else 0 end as ticket_deleted,
        first_value(valid_starting_at) over (partition by ticket_id, source_relation order by valid_starting_at desc, ticket_id rows unbounded preceding) as last_status_assignment_date,
        case when lag(status) over (partition by ticket_id, source_relation order by valid_starting_at) = 'deleted' and status != 'deleted'
            then 1
            else 0
                end as ticket_recoveries

    from ticket_historical_status

)

select 
  ticket_id,
  source_relation,
  last_status_assignment_date,
  sum(ticket_deleted) as ticket_deleted_count,
  sum(agent_wait_time_in_minutes) as agent_wait_time_in_calendar_minutes,
  sum(requester_wait_time_in_minutes) as requester_wait_time_in_calendar_minutes,
  sum(solve_time_in_minutes) as solve_time_in_calendar_minutes,
  sum(agent_work_time_in_minutes) as agent_work_time_in_calendar_minutes,
  sum(on_hold_time_in_minutes) as on_hold_time_in_calendar_minutes,
  sum(new_status_duration_minutes) as new_status_duration_in_calendar_minutes,
  sum(open_status_duration_minutes) as open_status_duration_in_calendar_minutes,
  sum(ticket_recoveries) as total_ticket_recoveries
from calendar_minutes
group by 1, 2, 3