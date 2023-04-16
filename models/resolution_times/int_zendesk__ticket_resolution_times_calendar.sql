with historical_solved_status as (

    select *
    from {{ ref('int_zendesk__ticket_historical_status') }}
    where status = 'solved'

), ticket as (

    select *
    from {{ ref('stg_zendesk__ticket') }}

), ticket_historical_assignee as (

    select *
    from {{ ref('int_zendesk__ticket_historical_assignee') }}

), ticket_historical_group as (

  select *
  from {{ ref('int_zendesk__ticket_historical_group') }}

), ticket_schedule as (

  select *
  from {{ ref('stg_zendesk__ticket_schedule') }}

), schedule_holiday as (

    select *
    from {{ ref('stg_zendesk__schedule_holiday') }}   

), solved_times as (
  
  select
  
    historical_solved_status.ticket_id,
    ticket_schedule.schedule_id,
    schedule_holiday.holiday_id,
    schedule_holiday.holiday_name,
    schedule_holiday.holiday_start_date_at,
    schedule_holiday.holiday_end_date_at,
    min(valid_starting_at) as first_solved_at,
    max(valid_starting_at) as last_solved_at,
    count(status) as solved_count 

  from historical_solved_status
  left join ticket_schedule
    on historical_solved_status.ticket_id = ticket_schedule.ticket_id
  join schedule_holiday
    on ticket_schedule.schedule_id = schedule_holiday.schedule_id
  group by 1,2,3,4,5,6

), resolution_times as (

  select

    ticket.ticket_id,
    ticket.created_at,
    solved_times.first_solved_at,
    solved_times.last_solved_at,
    solved_times.holiday_id,
    solved_times.holiday_name,
    solved_times.holiday_start_date_at,
    solved_times.holiday_end_date_at,
    ticket_historical_assignee.unique_assignee_count,
    ticket_historical_assignee.assignee_stations_count,
    ticket_historical_group.group_stations_count,
    ticket_historical_assignee.first_assignee_id,
    ticket_historical_assignee.last_assignee_id,
    ticket_historical_assignee.first_agent_assignment_date,
    ticket_historical_assignee.last_agent_assignment_date,
    ticket_historical_assignee.ticket_unassigned_duration_calendar_minutes,
    solved_times.solved_count as total_resolutions,
    case when solved_times.solved_count <= 1
      then 0
      else solved_times.solved_count - 1 --subtracting one as the first solve is not a reopen.
        end as count_reopens,

    {{ dbt.datediff(
        'ticket_historical_assignee.first_agent_assignment_date', 
        'solved_times.last_solved_at',
        'minute' ) }} as first_assignment_to_resolution_calendar_minutes,
    {{ dbt.datediff(
        'ticket_historical_assignee.last_agent_assignment_date', 
        'solved_times.last_solved_at',
        'minute' ) }} as last_assignment_to_resolution_calendar_minutes,
    {{ dbt.datediff(
        'ticket.created_at', 
        'solved_times.first_solved_at',
        'minute' ) }} as first_resolution_calendar_minutes,
    {{ dbt.datediff(
        'ticket.created_at', 
        'solved_times.last_solved_at',
        'minute') }} as final_resolution_calendar_minutes,


    {{ dbt.datediff('solved_times.holiday_start_date_at',
        'solved_times.holiday_end_date_at',
        'minute') }} as holiday_duration_minutes

  from ticket

  left join ticket_historical_assignee
    using(ticket_id)

  left join ticket_historical_group
    using(ticket_id)

  left join solved_times
    using(ticket_id)

)

select

  ticket_id,
  created_at,
  first_solved_at,
  last_solved_at,
  holiday_id,
  holiday_name,
  holiday_start_date_at,
  holiday_end_date_at,
  unique_assignee_count,
  assignee_stations_count,
  group_stations_count,
  first_assignee_id,
  last_assignee_id,
  first_agent_assignment_date,
  last_agent_assignment_date,
  ticket_unassigned_duration_calendar_minutes,
  total_resolutions,
  count_reopens,

  case 
    when holiday_start_date_at >= first_agent_assignment_date
    and holiday_start_date_at <= last_solved_at
    and holiday_end_date_at <= last_solved_at
  then (first_assignment_to_resolution_calendar_minutes - holiday_duration_minutes) 
  end as first_assignment_to_resolution_calendar_minutes,
  case 
    when holiday_start_date_at >= last_agent_assignment_date
    and holiday_start_date_at <= last_solved_at
    and holiday_end_date_at <= last_solved_at
  then (last_assignment_to_resolution_calendar_minutes - holiday_duration_minutes) 
  end as last_assignment_to_resolution_calendar_minutes,
  case
    when holiday_start_date_at >= created_at
    and holiday_start_date_at <= first_solved_at
    and holiday_end_date_at <= first_solved_at
  then (first_resolution_calendar_minutes - holiday_duration_minutes) 
  end as first_resolution_calendar_minutes,
  case 
    when holiday_start_date_at >= created_at
    and holiday_start_date_at <= last_solved_at
    and holiday_end_date_at <= last_solved_at
  then (final_resolution_calendar_minutes - holiday_duration_minutes) 
  end as final_resolution_calendar_minutes

from resolution_times