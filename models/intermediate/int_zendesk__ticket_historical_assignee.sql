with assignee_updates as (

    select *
    from {{ ref('int_zendesk__updates') }}
    where field_name = 'assignee_id'

), calculate_metrics as (
    select
        ticket_id,
        field_name as assignee_id,
        value,
        ticket_created_date,
        valid_starting_at,
        lag(valid_starting_at) over (partition by ticket_id order by valid_starting_at) as previous_update,
        lag(value) over (partition by ticket_id order by valid_starting_at) as previous_assignee,
        first_value(valid_starting_at) over (partition by ticket_id order by valid_starting_at, ticket_id rows unbounded preceding) as first_agent_assignment_date,
        first_value(value) over (partition by ticket_id order by valid_starting_at, ticket_id rows unbounded preceding) as first_assignee_id,
        first_value(valid_starting_at) over (partition by ticket_id order by valid_starting_at desc, ticket_id rows unbounded preceding) as last_agent_assignment_date,
        first_value(value) over (partition by ticket_id order by valid_starting_at desc, ticket_id rows unbounded preceding) as last_assignee_id,
        count(value) over (partition by ticket_id) as assignee_stations_count
    from assignee_updates

), unassigned_time as (
    select
        ticket_id,
        sum(case when assignee_id is not null and previous_assignee is null 
            then {{ dbt_utils.datediff("coalesce(previous_update, ticket_created_date)", "valid_starting_at", 'second') }} / 60
            else 0
                end) as ticket_unassigned_duration_calendar_minutes,
        count(distinct value) as unique_assignee_count
    from calculate_metrics

    group by 1

), window_group as (
    select
        calculate_metrics.ticket_id,
        calculate_metrics.first_agent_assignment_date,
        calculate_metrics.first_assignee_id,
        calculate_metrics.last_agent_assignment_date,
        calculate_metrics.last_assignee_id,
        calculate_metrics.assignee_stations_count
    from calculate_metrics

    {{ dbt_utils.group_by(n=6) }}

), final as (
    select
        window_group.*,
        unassigned_time.unique_assignee_count,
        unassigned_time.ticket_unassigned_duration_calendar_minutes
    from window_group

    left join unassigned_time
        using(ticket_id)
)

select *
from final