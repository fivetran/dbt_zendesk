with ticket_group_history as (

    select *
    from {{ ref('int_zendesk__updates') }}
    where field_name = 'group_id'

), group_breakdown as (
    select
  
        ticket_id,
        source_relation,
        valid_starting_at,
        valid_ending_at,
        value as group_id
    from ticket_group_history

), final as (
    select
        ticket_id,
        source_relation,
        count(group_id) as group_stations_count
    from group_breakdown

    group by 1, 2
)

select *
from final