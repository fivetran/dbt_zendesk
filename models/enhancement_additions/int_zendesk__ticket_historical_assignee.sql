-- To do -- can we delete ticket_status_counter and unique_status_counter?

with ticket_assignee_history as (

    select *
    from {{ ref('stg_zendesk__ticket_field_history') }}
    where field_name = 'assignee_id'

), assignee_breakdown as (
    select
  
        ticket_id,
        valid_starting_at,
        valid_ending_at,
        {{ timestamp_diff(
            'valid_starting_at',
            "coalesce(valid_ending_at, " ~ dbt_utils.current_timestamp() ~ ")",
            'minute') }} as status_duration_calendar_minutes,
        value as assignee_id
    from ticket_assignee_history

), unique_assignee_count as (
    select distinct
        ticket_id,
        count(assignee_id) as unique_assignee_count
    from assignee_breakdown

    group by 1

), assignee_station_count as (
    select
        ticket_id,
        count(assignee_id) as assignee_stations_count
    from assignee_breakdown

    group by 1

), first_assignee_starter as (
    select 
        ticket_id,
        min(valid_starting_at) as first_agent_assignment_date
    from assignee_breakdown

    group by 1

), last_assignee_starter as (
    select 
        ticket_id,
        max(valid_starting_at) as last_agent_assignment_date
    from assignee_breakdown

    group by 1

), first_assignee as (
    select 
        assignee_breakdown.ticket_id,
        assignee_breakdown.assignee_id as first_assignee_id,
        first_assignee_starter.first_agent_assignment_date
    from assignee_breakdown

    inner join first_assignee_starter
        on first_assignee_starter.ticket_id = assignee_breakdown.ticket_id
            and first_assignee_starter.first_agent_assignment_date = assignee_breakdown.valid_starting_at

), last_assignee as (
    select 
        assignee_breakdown.ticket_id,
        assignee_breakdown.assignee_id as last_assignee_id,
        last_assignee_starter.last_agent_assignment_date
    from assignee_breakdown

    inner join last_assignee_starter
        on last_assignee_starter.ticket_id = assignee_breakdown.ticket_id
            and last_assignee_starter.last_agent_assignment_date = assignee_breakdown.valid_starting_at

), final as (
    select
        first_assignee.ticket_id,
        unique_assignee_count.unique_assignee_count,
        assignee_station_count.assignee_stations_count,
        first_assignee.first_assignee_id,
        first_assignee.first_agent_assignment_date,
        last_assignee.last_assignee_id,
        last_assignee.last_agent_assignment_date
    from first_assignee

    left join last_assignee
        using(ticket_id)

    left join unique_assignee_count
        using(ticket_id)

    left join assignee_station_count
        using(ticket_id)
)

select *
from final