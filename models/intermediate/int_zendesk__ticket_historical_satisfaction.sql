{{ config(enabled=var('customer360__using_zendesk', true)) }}

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

), first_and_latest_score as (
    select
        ticket_id,
        first_value(value) over (partition by ticket_id order by valid_starting_at, ticket_id rows unbounded preceding) as first_satisfaction_score,
        first_value(value) over (partition by ticket_id order by valid_starting_at desc, ticket_id rows unbounded preceding) as latest_satisfaction_score
    from satisfaction_updates

    where field_name = 'satisfaction_score' and value != 'offered'

), satisfaction_scores as (
    select
        ticket_id,
        count(value) over (partition by ticket_id) as count_satisfaction_scores,
        case when lag(value) over (partition by ticket_id order by valid_starting_at desc) = 'good' and value = 'bad'
            then 1
            else 0
                end as good_to_bad_score,
        case when lag(value) over (partition by ticket_id order by valid_starting_at desc) = 'bad' and value = 'good'
            then 1
            else 0
                end as bad_to_good_score
    from satisfaction_updates
    where field_name = 'satisfaction_score'

), score_group as (
    select
        ticket_id,
        count_satisfaction_scores,
        sum(good_to_bad_score) as total_good_to_bad_score,
        sum(bad_to_good_score) as total_bad_to_good_score
    from satisfaction_scores

    group by 1, 2

), window_group as (
    select
        satisfaction_updates.ticket_id,
        latest_reason.latest_satisfaction_reason,
        latest_comment.latest_satisfaction_comment,
        first_and_latest_score.first_satisfaction_score,
        first_and_latest_score.latest_satisfaction_score,
        score_group.count_satisfaction_scores,
        score_group.total_good_to_bad_score,
        score_group.total_bad_to_good_score

    from satisfaction_updates

    left join latest_reason
        on satisfaction_updates.ticket_id = latest_reason.ticket_id

    left join latest_comment
        on satisfaction_updates.ticket_id = latest_comment.ticket_id

    left join first_and_latest_score
        on satisfaction_updates.ticket_id = first_and_latest_score.ticket_id

    left join score_group
        on satisfaction_updates.ticket_id = score_group.ticket_id

    group by 1, 2, 3, 4, 5, 6, 7, 8

), final as (
    select
        ticket_id,
        latest_satisfaction_reason,
        latest_satisfaction_comment,
        first_satisfaction_score,
        latest_satisfaction_score,
        case when count_satisfaction_scores > 0
            then (count_satisfaction_scores - 1) --Subtracting one as the first score is always "offered".
            else count_satisfaction_scores
                end as count_satisfaction_scores,
        case when total_good_to_bad_score > 0
            then true
            else false
                end as is_good_to_bad_satisfaction_score,
        case when total_bad_to_good_score > 0
            then true
            else false
                end as is_bad_to_good_satisfaction_score
    from window_group
)

select *
from final