with assignee_updates as (

    select *
    from {{ ref('stg_zendesk__ticket_field_history') }}
    where field_name = 'assignee_id'

), assignee_duration as (
    select
        *,
        {{ timestamp_diff(
            'valid_starting_at',
            "coalesce(valid_ending_at, " ~ dbt_utils.current_timestamp() ~ ")",
            'minute') }} as assignee_duration_calendar_minutes
    from assignee_updates

), calculate_metrics as (
    select
        ticket_id,
        first_value(valid_starting_at) over (partition by ticket_id order by valid_starting_at) as first_agent_assignment_date,
        first_value(value) over (partition by ticket_id order by valid_starting_at) as first_assignee_id,
        first_value(valid_starting_at) over (partition by ticket_id order by valid_starting_at desc) as last_agent_assignment_date,
        first_value(value) over (partition by ticket_id order by valid_starting_at desc) as last_assignee_id,
        --first_value(assignee_duration_calendar_minutes) over (partition by ticket_id order by valid_starting_at desc) as last_assignee_duration_calendar_minutes,
        count(distinct value) over (partition by ticket_id) as unique_assignee_count,
        count(value) over (partition by ticket_id) as assignee_stations_count
    from assignee_duration

), final as (
    select * 
    from calculate_metrics

    group by 1, 2, 3, 4, 5, 6, 7, 8
)

select *
from final