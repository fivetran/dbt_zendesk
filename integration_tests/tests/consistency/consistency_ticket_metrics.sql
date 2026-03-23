
{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

with prod as (
    select
        ticket_id,
        first_reply_time_business_minutes, 
        first_reply_time_calendar_minutes,
        count_public_agent_comments,
        count_agent_comments,
        count_end_user_comments,
        count_public_comments,
        count_internal_comments,
        total_comments,
        count_ticket_handoffs,
        total_agent_replies
    from {{ target.schema }}_zendesk_prod.zendesk__ticket_metrics
),

dev as (
    select
        ticket_id,
        first_reply_time_business_minutes, 
        first_reply_time_calendar_minutes,
        count_public_agent_comments,
        count_agent_comments,
        count_end_user_comments,
        count_public_comments,
        count_internal_comments,
        total_comments,
        count_ticket_handoffs,
        total_agent_replies
    from {{ target.schema }}_zendesk_dev.zendesk__ticket_metrics

    {# Make sure we're only comparing one schema since this current update (v0.19.0) added mult-schema support. Can remove for future releases #}
    {{ "where source_relation =  '" ~ (var("zendesk_database", target.database)|lower ~ "." ~ var("zendesk_schema", "zendesk")) ~ "'" if 'source_relation' in var("consistency_test_exclude_fields", '[]') }}
),

final as (
    select 
        prod.ticket_id,
        prod.first_reply_time_business_minutes as prod_first_reply_time_business_minutes,
        dev.first_reply_time_business_minutes as dev_first_reply_time_business_minutes,
        prod.first_reply_time_calendar_minutes as prod_first_reply_time_calendar_minutes,
        dev.first_reply_time_calendar_minutes as dev_first_reply_time_calendar_minutes,
        prod.count_public_agent_comments as prod_count_public_agent_comments,
        dev.count_public_agent_comments as dev_count_public_agent_comments,
        prod.count_agent_comments as prod_count_agent_comments,
        dev.count_agent_comments as dev_count_agent_comments,
        prod.count_end_user_comments as prod_count_end_user_comments,
        dev.count_end_user_comments as dev_count_end_user_comments,
        prod.count_public_comments as prod_count_public_comments,
        dev.count_public_comments as dev_count_public_comments,
        prod.count_internal_comments as prod_count_internal_comments,
        dev.count_internal_comments as dev_count_internal_comments,
        prod.total_comments as prod_total_comments,
        dev.total_comments as dev_total_comments,
        prod.count_ticket_handoffs as prod_count_ticket_handoffs,
        dev.count_ticket_handoffs as dev_count_ticket_handoffs,
        prod.total_agent_replies as prod_total_agent_replies,
        dev.total_agent_replies as dev_total_agent_replies
    from prod
    full outer join dev 
        on dev.ticket_id = prod.ticket_id
)

select *
from final
where (abs(prod_first_reply_time_business_minutes - dev_first_reply_time_business_minutes) >= 5
    or abs(prod_first_reply_time_calendar_minutes - dev_first_reply_time_calendar_minutes) >= 5
    or prod_count_public_agent_comments != dev_count_public_agent_comments
    or prod_count_agent_comments != dev_count_agent_comments
    or prod_count_end_user_comments != dev_count_end_user_comments
    or prod_count_public_comments != dev_count_public_comments
    or prod_count_internal_comments != dev_count_internal_comments
    or prod_total_comments != dev_total_comments
    or prod_count_ticket_handoffs != dev_count_ticket_handoffs
    or prod_total_agent_replies != dev_total_agent_replies)
    {{ "and ticket_id not in " ~ var('fivetran_consistency_ticket_metrics_exclusion_tickets',[]) ~ "" if var('fivetran_consistency_ticket_metrics_exclusion_tickets',[]) }}