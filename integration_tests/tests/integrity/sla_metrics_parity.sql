{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

/*
This test is to ensure the sla_elapsed_time from zendesk__sla_policies matches the corresponding time in zendesk__ticket_metrics.
*/

with slas as (
    select *
    from {{ target.schema }}_zendesk_dev.zendesk__sla_policies
    where in_business_hours

), metrics as (
    select *
    from {{ target.schema }}_zendesk_dev.zendesk__ticket_metrics

), comparison as (
    select 
        slas.ticket_id,
        slas.metric,
        cast(slas.sla_elapsed_time as {{ dbt.type_int() }}) as time_from_slas,
        case when slas.metric = 'agent_work_time' then metrics.agent_work_time_in_business_minutes
            when slas.metric = 'requester_wait_time' then metrics.requester_wait_time_in_business_minutes
            when slas.metric = 'first_reply_time' then metrics.first_reply_time_business_minutes
        end as time_from_metrics
    from slas
    left join metrics
        on metrics.ticket_id = slas.ticket_id
)

select *
from comparison
where abs(time_from_slas - time_from_metrics) >= 5
and ticket_id not in {{ var('fivetran_integrity_sla_metric_parity_exclusion_tickets', ()) }}