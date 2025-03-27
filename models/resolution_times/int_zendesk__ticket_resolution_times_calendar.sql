with historical_solved_status as (

    select 
      *,
      row_number() over (partition by source_relation, ticket_id order by valid_starting_at asc) as row_num
    from {{ ref('int_zendesk__ticket_historical_status') }}
    where status in ('solved', 'closed') -- Ideally we are looking for solved timestamps, but Zendesk sometimes (very infrequently) closes tickets without marking them as solved

), latest_solved_status as (

    select *
    from historical_solved_status
    where row_num = 1 -- Take the EARLIEST timestamp so that we grab solved records if possible. Otherwise the first closed record.

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
    source_relation,
    ticket_id,
    min(valid_starting_at) as first_solved_at,
    max(valid_starting_at) as last_solved_at,
    count(status) as solved_count 

  from latest_solved_status
  group by 1, 2

)

  select
    ticket.source_relation,
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
    on ticket.ticket_id = ticket_historical_assignee.ticket_id
    and ticket.source_relation = ticket_historical_assignee.source_relation

  left join ticket_historical_group
    on ticket.ticket_id = ticket_historical_group.ticket_id
    and ticket.source_relation = ticket_historical_group.source_relation

  left join solved_times
    on ticket.ticket_id = solved_times.ticket_id
    and ticket.source_relation = solved_times.source_relation

