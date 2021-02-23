with assignee_updates as (

    select *
    from {{ ref('stg_zendesk__ticket_field_history') }}
    where field_name = 'assignee_id'

), calculate_metrics as (
    select
        ticket_id,
        {{ fivetran_utils.first_value("valid_starting_at", "ticket_id", "valid_starting_at") }} as first_agent_assignment_date,
        {{ fivetran_utils.first_value("value", "ticket_id", "valid_starting_at") }} as first_assignee_id,
        {{ fivetran_utils.first_value("valid_starting_at", "ticket_id", "valid_starting_at", "desc") }} as last_agent_assignment_date,
        {{ fivetran_utils.first_value("value", "ticket_id", "valid_starting_at", "desc") }} as last_assignee_id,
        count(value) over (partition by ticket_id) as assignee_stations_count
    from assignee_updates

), distinct_count as (
    select distinct
        ticket_id,
        count(distinct value) as unique_assignee_count
    from assignee_updates

    group by 1

), final as (
    select 
        calculate_metrics.*,
        distinct_count.unique_assignee_count
    from calculate_metrics

    left join distinct_count
        using(ticket_id)

    group by 1, 2, 3, 4, 5, 6, 7
)

select *
from final