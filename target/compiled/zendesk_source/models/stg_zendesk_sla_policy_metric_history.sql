

with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`sla_policy_metric_history`

), fields as (
    
    select
      sla_policy_id,
      sla_policy_updated_at,
      coalesce(
        lead(sla_policy_updated_at) over (partition by sla_policy_id, business_hours, metric, priority order by sla_policy_updated_at),
        '2999-12-31 23:59:59 UTC')
        as valid_ending_at,
      business_hours, 
      metric, 
      priority, 
      target

    from base
)

select *
from fields