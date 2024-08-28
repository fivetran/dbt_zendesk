
{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

with prod as (
    select
        1 as join_key,
        count(*) as total_slas
    from {{ target.schema }}_zendesk_prod.zendesk__sla_policies
    where prod.ticket_id not in {{ var('fivetran_consistency_sla_policy_count_exclusion_tickets',()) }}
    group by 1
),

dev as (
    select
        1 as join_key,
        count(*) as total_slas
    from {{ target.schema }}_zendesk_dev.zendesk__sla_policies
    where dev.ticket_id not in {{ var('fivetran_consistency_sla_policy_count_exclusion_tickets',()) }}
    group by 1
),

final as (
    select 
        prod.join_key,
        prod.total_slas as prod_sla_total,
        dev.total_slas as dev_sla_total
    from prod
    full outer join dev 
        on dev.join_key = prod.join_key
)

select *
from final
where prod_sla_total != dev_sla_total
