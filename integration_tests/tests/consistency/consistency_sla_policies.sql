
{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

with prod as (
    select
        ticket_id,
        metric, 
        sla_applied_at,
        sla_elapsed_time,
        is_sla_breach
    from {{ target.schema }}_zendesk_prod.zendesk__sla_policies
),

dev as (
    select
        ticket_id,
        metric, 
        sla_applied_at,
        sla_elapsed_time,
        is_sla_breach
    from {{ target.schema }}_zendesk_dev.zendesk__sla_policies
),

final as (
    select 
        prod.ticket_id,
        prod.metric,
        prod.sla_applied_at,
        prod.sla_elapsed_time as prod_sla_elapsed_time,
        dev.sla_elapsed_time as dev_sla_elapsed_time,
        prod.is_sla_breach as prod_is_sla_breach,
        dev.is_sla_breach as dev_is_sla_breach
    from prod
    full outer join dev 
        on dev.ticket_id = prod.ticket_id
            and dev.metric = prod.metric
            and dev.sla_applied_at = prod.sla_applied_at
)

select *
from final
where (abs(prod_sla_elapsed_time - dev_sla_elapsed_time) >= 5
    or prod_is_sla_breach != dev_is_sla_breach)
    {{ "and prod.ticket_id not in " ~ var('fivetran_consistency_sla_policies_exclusion_tickets',[]) ~ "" if var('fivetran_consistency_sla_policies_exclusion_tickets',[]) }}