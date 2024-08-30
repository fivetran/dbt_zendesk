
{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

with prod as (
    select 
        ticket_id,
        sla_policy_name,
        metric,
        sla_applied_at,
        target,
        in_business_hours,
        sla_breach_at,
        round(sla_elapsed_time, -1) as sla_elapsed_time, --round to the nearest tens
        is_active_sla,
        is_sla_breach
    from {{ target.schema }}_zendesk_prod.zendesk__sla_policies
),

dev as (
    select
        ticket_id,
        sla_policy_name,
        metric,
        sla_applied_at,
        target,
        in_business_hours,
        sla_breach_at,
        round(sla_elapsed_time, -1) as sla_elapsed_time, --round to the nearest tens
        is_active_sla,
        is_sla_breach
    from {{ target.schema }}_zendesk_dev.zendesk__sla_policies
),

prod_not_in_dev as (
    -- rows from prod not found in dev
    select * from prod
    except distinct
    select * from dev
),

dev_not_in_prod as (
    -- rows from dev not found in prod
    select * from dev
    except distinct
    select * from prod
),

final as (
    select
        *,
        'from prod' as source
    from prod_not_in_dev

    union all -- union since we only care if rows are produced

    select
        *,
        'from dev' as source
    from dev_not_in_prod
)

select *
from final
{{ "where ticket_id not in " ~ var('fivetran_consistency_sla_policies_exclusion_tickets',[]) ~ "" if var('fivetran_consistency_sla_policies_exclusion_tickets',[]) }}