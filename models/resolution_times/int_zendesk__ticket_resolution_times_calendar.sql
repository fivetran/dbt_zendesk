with historical_solved_status as (

    select *
    from {{ ref('int_zendesk__ticket_historical_status') }}
    where status = 'solved'

), ticket as (

    select *
    from {{ ref('stg_zendesk__ticket') }}

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
    {{ timestamp_diff(
        'ticket.created_at', 
        'solved_times.first_solved_at',
        'minute' ) }} as first_resolution_calendar_minutes,
    {{ timestamp_diff(
        'ticket.created_at', 
        'solved_times.last_solved_at',
        'minute') }} as final_resolution_calendar_minutes

  from ticket
  left join solved_times
    on solved_times.ticket_id = ticket.ticket_id

