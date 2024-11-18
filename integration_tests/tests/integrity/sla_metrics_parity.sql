{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

/*
This test is to ensure the sla_elapsed_time from zendesk__sla_policies matches the corresponding time in zendesk__ticket_metrics.
*/

with dev_slas as (
    select *
    from {{ target.schema }}_zendesk_dev.zendesk__sla_policies
    where in_business_hours

), dev_metrics as (
    select *
    from {{ target.schema }}_zendesk_dev.zendesk__ticket_metrics

), dev_compare as (
    select 
        dev_slas.source_relation,
        dev_slas.ticket_id,
        dev_slas.metric,
        cast(dev_slas.sla_elapsed_time as {{ dbt.type_int() }}) as time_from_slas,
        case when metric = 'agent_work_time' then dev_metrics.agent_work_time_in_business_minutes
            when metric = 'requester_wait_time' then dev_metrics.requester_wait_time_in_business_minutes
            when metric = 'first_reply_time' then dev_metrics.first_reply_time_business_minutes
        end as time_from_metrics
    from dev_slas
    left join dev_metrics
        on dev_metrics.ticket_id = dev_slas.ticket_id
        and dev_metrics.source_relation = dev_slas.source_relation
)

select *
from dev_compare
where abs(time_from_slas - time_from_metrics) >= 5
{{ "and ticket_id not in " ~ var('fivetran_integrity_sla_metric_parity_exclusion_tickets',[]) ~ "" if var('fivetran_integrity_sla_metric_parity_exclusion_tickets',[]) }}