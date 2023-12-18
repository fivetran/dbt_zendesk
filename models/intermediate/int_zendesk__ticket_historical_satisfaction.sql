with satisfaction_updates as (

    select *
    from {{ ref('int_zendesk__updates') }}
    where field_name in ('satisfaction_score', 'satisfaction_comment', 'satisfaction_reason_code') 

), latest_reason as (
    select
        ticket_id,
        source_relation,
        first_value(value) over (partition by ticket_id, source_relation order by valid_starting_at desc, ticket_id rows unbounded preceding) as latest_satisfaction_reason
    from satisfaction_updates

    where field_name = 'satisfaction_reason_code'

), latest_comment as (
    select
        ticket_id,
        source_relation,
        first_value(value) over (partition by ticket_id, source_relation order by valid_starting_at desc, ticket_id rows unbounded preceding) as latest_satisfaction_comment
    from satisfaction_updates

    where field_name = 'satisfaction_comment'

), first_and_latest_score as (
    select
        ticket_id,
        source_relation,
        first_value(value) over (partition by ticket_id, source_relation order by valid_starting_at, ticket_id rows unbounded preceding) as first_satisfaction_score,
        first_value(value) over (partition by ticket_id, source_relation order by valid_starting_at desc, ticket_id rows unbounded preceding) as latest_satisfaction_score
    from satisfaction_updates

    where field_name = 'satisfaction_score' and value != 'offered'

), satisfaction_scores as (
    select
        ticket_id,
        source_relation,
        count(value) over (partition by ticket_id, source_relation) as count_satisfaction_scores,
        case when lag(value) over (partition by ticket_id, source_relation order by valid_starting_at desc) = 'good' and value = 'bad'
            then 1
            else 0
                end as good_to_bad_score,
        case when lag(value) over (partition by ticket_id, source_relation order by valid_starting_at desc) = 'bad' and value = 'good'
            then 1
            else 0
                end as bad_to_good_score
    from satisfaction_updates
    where field_name = 'satisfaction_score'

), score_group as (
    select
        ticket_id,
        source_relation,
        count_satisfaction_scores,
        sum(good_to_bad_score) as total_good_to_bad_score,
        sum(bad_to_good_score) as total_bad_to_good_score
    from satisfaction_scores

    group by 1, 2, 3

), window_group as (
    select
        satisfaction_updates.ticket_id,
        satisfaction_updates.source_relation,
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
        and satisfaction_updates.source_relation = latest_reason.source_relation

    left join latest_comment
        on satisfaction_updates.ticket_id = latest_comment.ticket_id
        and satisfaction_updates.source_relation = latest_comment.source_relation

    left join first_and_latest_score
        on satisfaction_updates.ticket_id = first_and_latest_score.ticket_id
        and satisfaction_updates.source_relation = first_and_latest_score.source_relation

    left join score_group
        on satisfaction_updates.ticket_id = score_group.ticket_id
        and satisfaction_updates.source_relation = score_group.source_relation

    {{ dbt_utils.group_by(n=9) }}

), final as (
    select
        ticket_id,
        source_relation,
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