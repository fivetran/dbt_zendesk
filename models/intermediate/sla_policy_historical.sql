{{ config(enabled=var('using_sla_policy', True)) }}

with sla_policy_history as (

    select 
      sla_policy_id,
      created_at
    from {{ ref('stg_zendesk_sla_policy_history') }}
    group by 1,2

), sla_policy_metric_history as (

    select 
      *,
      row_number () over (partition by sla_policy_id, business_hours, metric, priority order by sla_policy_updated_at) as revision_number
    from {{ ref('stg_zendesk_sla_policy_metric_history') }}

)

select
    
    sla_policy_metric_history.sla_policy_id,
    case when sla_policy_metric_history.revision_number = 1 then sla_policy_history.created_at
      else sla_policy_metric_history.sla_policy_updated_at end as valid_starting_at,
    sla_policy_metric_history.valid_ending_at,
    sla_policy_metric_history.business_hours, 
    sla_policy_metric_history.metric, 
    sla_policy_metric_history.priority, 
    sla_policy_metric_history.target

from sla_policy_metric_history
join sla_policy_history
    on sla_policy_history.sla_policy_id = sla_policy_metric_history.sla_policy_id