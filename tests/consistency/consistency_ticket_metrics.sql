
{{ config(
    tags="validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

with prod as (
    select
        ticket_id,
        first_reply_time_business_minutes, 
        first_reply_time_calendar_minutes
    from {{ target.schema }}_zendesk_prod.zendesk__ticket_metrics
),

dev as (
    select
        ticket_id,
        first_reply_time_business_minutes, 
        first_reply_time_calendar_minutes
    from {{ target.schema }}_zendesk_dev.zendesk__ticket_metrics
),

final as (
    select 
        prod.ticket_id,
        prod.first_reply_time_business_minutes as prod_first_reply_time_business_minutes,
        dev.first_reply_time_business_minutes as dev_first_reply_time_business_minutes,
        prod.first_reply_time_calendar_minutes as prod_first_reply_time_calendar_minutes,
        dev.first_reply_time_calendar_minutes as dev_first_reply_time_calendar_minutes
    from prod
    full outer join dev 
        on dev.ticket_id = prod.ticket_id
)

select *
from final
where (abs(prod_first_reply_time_business_minutes - dev_first_reply_time_business_minutes) >= 5
    or abs(prod_first_reply_time_calendar_minutes - dev_first_reply_time_calendar_minutes) >= 5)
    {{ "and ticket_id not in " ~ var('fivetran_consistency_ticket_metrics_exclusion_tickets',[]) ~ "" if var('fivetran_consistency_ticket_metrics_exclusion_tickets',[]) }}