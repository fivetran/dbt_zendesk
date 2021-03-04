with ticket_metrics as (
    select *
    from {{ ref('zendesk__ticket_metrics') }}

), user_table as (
    select *
    from {{ ref('stg_zendesk__user') }}

), user_sum as (
    select
        cast(1 as {{ dbt_utils.type_int() }}) as summary_helper,
        sum(case when is_active = true
            then 1
            else 0
                end) as user_count,
        sum(case when lower(role) != 'end-user' and is_active = true
            then 1
            else 0
                end) as active_agent_count,
        sum(case when is_active = false
            then 1
            else 0
                end) as deleted_user_count,
        sum(case when lower(role) = 'end-user' and is_active = true
            then 1
            else 0
                end) as end_user_count,
        sum(case when is_suspended = true
            then 1
            else 0
                end) as suspended_user_count
    from user_table

    group by 1

), ticket_metric_sum as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as summary_helper,
        sum(case when lower(status) = 'new'
            then 1
            else 0
                end) as new_ticket_count,
        sum(case when lower(status) = 'hold'
            then 1
            else 0
                end) as on_hold_ticket_count,
        sum(case when lower(status) = 'open'
            then 1
            else 0
                end) as open_ticket_count,
        sum(case when lower(status) = 'pending'
            then 1
            else 0
                end) as pending_ticket_count,
        sum(case when lower(type) = 'problem'
            then 1
            else 0
                end) as problem_ticket_count,
        sum(case when first_assignee_id != last_assignee_id
            then 1
            else 0
                end) as reassigned_ticket_count,
        sum(case when count_reopens > 0
            then 1
            else 0
                end) as reopened_ticket_count,

        sum(case when lower(ticket_satisfaction_score) in ('offered', 'good', 'bad')
            then 1
            else 0
                end) as surveyed_satisfaction_ticket_count,

        sum(case when assignee_id is null and lower(status) not in ('solved', 'closed')
            then 1
            else 0
                end) as unassigned_unsolved_ticket_count,
        sum(case when total_agent_replies < 0
            then 1
            else 0
                end) as unreplied_ticket_count,
        sum(case when total_agent_replies < 0 and lower(status) not in ('solved', 'closed')
            then 1
            else 0
                end) as unreplied_unsolved_ticket_count,
        sum(case when lower(status) not in ('solved', 'closed')
            then 1
            else 0
                end) as unsolved_ticket_count,
        sum(case when lower(status) in ('solved', 'closed')
            then 1
            else 0
                end) as solved_ticket_count,
        sum(case when lower(status) in ('deleted')
            then 1
            else 0
                end) as deleted_ticket_count,
        sum(case when total_ticket_recoveries > 0
            then 1
            else 0
                end) as recovered_ticket_count,
        sum(case when assignee_stations_count > 0
            then 1
            else 0
                end) as assigned_ticket_count,
        count(count_internal_comments) as total_internal_comments,
        count(count_public_comments) as total_public_comments,
        count(total_comments)
    from ticket_metrics
    
    group by 1


), final as (
    select
        user_sum.user_count,
        user_sum.active_agent_count,
        user_sum.deleted_user_count,
        user_sum.end_user_count,
        user_sum.suspended_user_count,
        ticket_metric_sum.new_ticket_count,
        ticket_metric_sum.on_hold_ticket_count,
        ticket_metric_sum.open_ticket_count,
        ticket_metric_sum.pending_ticket_count,
        ticket_metric_sum.solved_ticket_count,
        ticket_metric_sum.problem_ticket_count,
        ticket_metric_sum.assigned_ticket_count,
        ticket_metric_sum.reassigned_ticket_count,
        ticket_metric_sum.reopened_ticket_count,
        ticket_metric_sum.surveyed_satisfaction_ticket_count,
        ticket_metric_sum.unassigned_unsolved_ticket_count,
        ticket_metric_sum.unreplied_ticket_count,
        ticket_metric_sum.unreplied_unsolved_ticket_count,
        ticket_metric_sum.unsolved_ticket_count,
        ticket_metric_sum.recovered_ticket_count,
        ticket_metric_sum.deleted_ticket_count
    from user_sum

    left join ticket_metric_sum
        using(summary_helper)
)

select *
from final