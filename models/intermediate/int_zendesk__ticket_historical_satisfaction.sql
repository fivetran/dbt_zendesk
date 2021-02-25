with satisfaction_updates as (

    select *
    from {{ ref('int_zendesk__updates') }}
    where field_name in ('satisfaction_score', 'satisfaction_comment', 'satisfaction_reason_code') 

), latest_reason as (
    select
        ticket_id,
        first_value(value) over (partition by ticket_id order by valid_starting_at desc, ticket_id rows unbounded preceding) as latest_satisfaction_reason
    from satisfaction_updates

    where field_name = 'satisfaction_reason_code'

), latest_comment as (
    select
        ticket_id,
        first_value(value) over (partition by ticket_id order by valid_starting_at desc, ticket_id rows unbounded preceding) as latest_satisfaction_comment
    from satisfaction_updates

    where field_name = 'satisfaction_comment'

), latest_score as (
    select
        ticket_id,
        first_value(value) over (partition by ticket_id order by valid_starting_at desc, ticket_id rows unbounded preceding) as latest_satisfaction_score
    from satisfaction_updates

    where field_name = 'satisfaction_score'

), final as (
    select
        satisfaction_updates.ticket_id,
        latest_reason.latest_satisfaction_reason,
        latest_comment.latest_satisfaction_comment,
        latest_score.latest_satisfaction_score
    from satisfaction_updates

    left join latest_reason
        using(ticket_id)

    left join latest_comment
        using(ticket_id)

    left join latest_score
        using(ticket_id)

    group by 1, 2, 3, 4
)

select *
from final