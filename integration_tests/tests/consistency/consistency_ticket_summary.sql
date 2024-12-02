
{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

with prod as (
    select
        user_count,
        active_agent_count,
        deleted_user_count,
        end_user_count,
        suspended_user_count,
        new_ticket_count,
        on_hold_ticket_count,
        open_ticket_count,
        pending_ticket_count,
        solved_ticket_count,
        problem_ticket_count,
        assigned_ticket_count,
        reassigned_ticket_count,
        reopened_ticket_count,
        surveyed_satisfaction_ticket_count,
        unassigned_unsolved_ticket_count,
        unreplied_ticket_count,
        unreplied_unsolved_ticket_count,
        unsolved_ticket_count,
        recovered_ticket_count,
        deleted_ticket_count

    from {{ target.schema }}_zendesk_prod.zendesk__ticket_summary
),

dev as (
    select
        user_count,
        active_agent_count,
        deleted_user_count,
        end_user_count,
        suspended_user_count,
        new_ticket_count,
        on_hold_ticket_count,
        open_ticket_count,
        pending_ticket_count,
        solved_ticket_count,
        problem_ticket_count,
        assigned_ticket_count,
        reassigned_ticket_count,
        reopened_ticket_count,
        surveyed_satisfaction_ticket_count,
        unassigned_unsolved_ticket_count,
        unreplied_ticket_count,
        unreplied_unsolved_ticket_count,
        unsolved_ticket_count,
        recovered_ticket_count,
        deleted_ticket_count

    from {{ target.schema }}_zendesk_dev.zendesk__ticket_summary

    {# Make sure we're only comparing one schema since this current update (v0.19.0) added mult-schema support. Can remove for future releases #}
    {{ "where source_relation =  '" ~ (var("zendesk_database", target.database)|lower ~ "." ~ var("zendesk_schema", "zendesk")) ~ "'" if 'source_relation' in var("consistency_test_exclude_fields", '[]') }}
),

joined as (

    select 
        prod.user_count as prod_user_count,
        dev.user_count as dev_user_count,
        prod.active_agent_count as prod_active_agent_count,
        dev.active_agent_count as dev_active_agent_count,
        prod.deleted_user_count as prod_deleted_user_count,
        dev.deleted_user_count as dev_deleted_user_count,
        prod.end_user_count as prod_end_user_count,
        dev.end_user_count as dev_end_user_count,
        prod.suspended_user_count as prod_suspended_user_count,
        dev.suspended_user_count as dev_suspended_user_count,
        prod.new_ticket_count as prod_new_ticket_count,
        dev.new_ticket_count as dev_new_ticket_count,
        prod.on_hold_ticket_count as prod_on_hold_ticket_count,
        dev.on_hold_ticket_count as dev_on_hold_ticket_count,
        prod.open_ticket_count as prod_open_ticket_count,
        dev.open_ticket_count as dev_open_ticket_count,
        prod.pending_ticket_count as prod_pending_ticket_count,
        dev.pending_ticket_count as dev_pending_ticket_count,
        prod.solved_ticket_count as prod_solved_ticket_count,
        dev.solved_ticket_count as dev_solved_ticket_count,
        prod.problem_ticket_count as prod_problem_ticket_count,
        dev.problem_ticket_count as dev_problem_ticket_count,
        prod.assigned_ticket_count as prod_assigned_ticket_count,
        dev.assigned_ticket_count as dev_assigned_ticket_count,
        prod.reassigned_ticket_count as prod_reassigned_ticket_count,
        dev.reassigned_ticket_count as dev_reassigned_ticket_count,
        prod.reopened_ticket_count as prod_reopened_ticket_count,
        dev.reopened_ticket_count as dev_reopened_ticket_count,
        prod.surveyed_satisfaction_ticket_count as prod_surveyed_satisfaction_ticket_count,
        dev.surveyed_satisfaction_ticket_count as dev_surveyed_satisfaction_ticket_count,
        prod.unassigned_unsolved_ticket_count as prod_unassigned_unsolved_ticket_count,
        dev.unassigned_unsolved_ticket_count as dev_unassigned_unsolved_ticket_count,
        prod.unreplied_ticket_count as prod_unreplied_ticket_count,
        dev.unreplied_ticket_count as dev_unreplied_ticket_count,
        prod.unreplied_unsolved_ticket_count as prod_unreplied_unsolved_ticket_count,
        dev.unreplied_unsolved_ticket_count as dev_unreplied_unsolved_ticket_count,
        prod.unsolved_ticket_count as prod_unsolved_ticket_count,
        dev.unsolved_ticket_count as dev_unsolved_ticket_count,
        prod.recovered_ticket_count as prod_recovered_ticket_count,
        dev.recovered_ticket_count as dev_recovered_ticket_count,
        prod.deleted_ticket_count as prod_deleted_ticket_count,
        dev.deleted_ticket_count as dev_deleted_ticket_count

    from prod 
    cross join dev 
)

select *
from joined 
where -- sometimes one of the below metrics will be off by 6-8, but let's leave 5 for now
    abs(prod_user_count - dev_user_count) > 5
    or abs(prod_active_agent_count - dev_active_agent_count) > 5
    or abs(prod_deleted_user_count - dev_deleted_user_count) > 5
    or abs(prod_end_user_count - dev_end_user_count) > 5
    or abs(prod_suspended_user_count - dev_suspended_user_count) > 5
    or abs(prod_new_ticket_count - dev_new_ticket_count) > 5
    or abs(prod_on_hold_ticket_count - dev_on_hold_ticket_count) > 5
    or abs(prod_open_ticket_count - dev_open_ticket_count) > 8
    or abs(prod_pending_ticket_count - dev_pending_ticket_count) > 5
    or abs(prod_solved_ticket_count - dev_solved_ticket_count) > 5
    or abs(prod_problem_ticket_count - dev_problem_ticket_count) > 5
    or abs(prod_assigned_ticket_count - dev_assigned_ticket_count) > 5
    or abs(prod_reassigned_ticket_count - dev_reassigned_ticket_count) > 5
    or abs(prod_reopened_ticket_count - dev_reopened_ticket_count) > 5
    or abs(prod_surveyed_satisfaction_ticket_count - dev_surveyed_satisfaction_ticket_count) > 5
    or abs(prod_unassigned_unsolved_ticket_count - dev_unassigned_unsolved_ticket_count) > 5
    or abs(prod_unreplied_ticket_count - dev_unreplied_ticket_count) > 5
    or abs(prod_unreplied_unsolved_ticket_count - dev_unreplied_unsolved_ticket_count) > 5
    or abs(prod_unsolved_ticket_count - dev_unsolved_ticket_count) > 5
    or abs(prod_recovered_ticket_count - dev_recovered_ticket_count) > 5
    or abs(prod_deleted_ticket_count - dev_deleted_ticket_count) > 5