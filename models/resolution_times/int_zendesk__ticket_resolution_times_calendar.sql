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

), solved_times as (
  
  select
  
    ticket_id,
    min(valid_starting_at) as first_solved_at,
    max(valid_starting_at) as last_solved_at,
    count(status) as solved_count 

  from historical_solved_status
  group by 1

)

  select

    ticket.ticket_id,
    ticket.created_at,
    solved_times.first_solved_at,
    solved_times.last_solved_at,
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
        'minute') }} as final_resolution_calendar_minutes

  from ticket

  left join ticket_historical_assignee
    using(ticket_id)

  left join ticket_historical_group
    using(ticket_id)

  left join solved_times
    using(ticket_id)

