
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
        sla_elapsed_time,
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
        sla_elapsed_time,
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

combine as (
    select
        *,
        'from prod' as source
    from prod_not_in_dev

    union all -- union since we only care if rows are produced

    select
        *,
        'from dev' as source
    from dev_not_in_prod
),

final as (
    select 
        *,
        max(sla_elapsed_time) over (partition by ticket_id, metric, sla_applied_at) as max_sla_elapsed_time,
        min(sla_elapsed_time) over (partition by ticket_id, metric, sla_applied_at) as min_sla_elapsed_time,

        {# 
        This is necessary for upgrading to v0.18.1, as it introduces a fix for erronesouly null sla_policy_name values. The union all will consider these distinct rows as a result
        Remove this and following where clause afterward 
        #}
        sum(case when sla_policy_name is null then 1 else 0 end) over (partition by ticket_id, metric, sla_applied_at) = 1 as name_was_null_prior

    from combine 
    {{ "where ticket_id not in " ~ var('fivetran_consistency_sla_policies_exclusion_tickets',[]) ~ "" if var('fivetran_consistency_sla_policies_exclusion_tickets',[]) }}
)

select *
from final
where 
    {# Take differences in runtime into account #}
    max_sla_elapsed_time - min_sla_elapsed_time > 2 
    
    {# Remove after v0.18.1 #}
    and NOT name_was_null_prior