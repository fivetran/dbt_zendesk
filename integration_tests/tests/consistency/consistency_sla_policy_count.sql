
{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

with prod as (
    select
        ticket_id,
        count(*) as total_slas
    from {{ target.schema }}_zendesk_prod.zendesk__sla_policies
    where date(sla_applied_at) < current_date
    {{ "and ticket_id not in " ~ var('fivetran_consistency_sla_policy_count_exclusion_tickets',[]) ~ "" if var('fivetran_consistency_sla_policy_count_exclusion_tickets',[]) }}
    group by 1
),

dev as (
    select
        ticket_id,
        count(*) as total_slas
    from {{ target.schema }}_zendesk_dev.zendesk__sla_policies
    where date(sla_applied_at) < current_date
    {{ "and ticket_id not in " ~ var('fivetran_consistency_sla_policy_count_exclusion_tickets',[]) ~ "" if var('fivetran_consistency_sla_policy_count_exclusion_tickets',[]) }}
    group by 1
),

final as (
    select 
        prod.ticket_id as prod_ticket_id,
        dev.ticket_id as dev_ticket_id,
        prod.total_slas as prod_sla_total,
        dev.total_slas as dev_sla_total
    from prod
    full outer join dev 
        on dev.ticket_id = prod.ticket_id
)

select *
from final
where prod_sla_total != dev_sla_total