

with  __dbt__CTE__stg_zendesk_ticket_sla_policy as (


with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`ticket_sla_policy`

), fields as (
    
    select
      
      sla_policy_id,
      ticket_id,
      policy_applied_at

    from base
)

select *
from fields
),  __dbt__CTE__stg_zendesk_ticket_field_history as (
with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`ticket_field_history`

), fields as (
    
    select
    
      ticket_id,
      field_name,
      updated as valid_starting_at,
      lead(updated) over (partition by ticket_id, field_name order by updated) as valid_ending_at,
      value,
      user_id

    from base
    order by 1,2,3

)

select *
from fields
),  __dbt__CTE__stg_zendesk_sla_policy_history as (


with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`sla_policy_history`

), fields as (
    
    select

      id as sla_policy_id,
      _fivetran_deleted,
      created_at,
      updated_at,
      description,
      title
      
      
    from base

)

select *
from fields
),  __dbt__CTE__stg_zendesk_sla_policy_metric_history as (


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
),  __dbt__CTE__sla_policy_historical as (


with sla_policy_history as (

    select 
      sla_policy_id,
      created_at
    from __dbt__CTE__stg_zendesk_sla_policy_history
    group by 1,2

), sla_policy_metric_history as (

    select 
      *,
      row_number () over (partition by sla_policy_id, business_hours, metric, priority order by sla_policy_updated_at) as revision_number
    from __dbt__CTE__stg_zendesk_sla_policy_metric_history

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
),ticket_sla_policy as (

    select *
    from __dbt__CTE__stg_zendesk_ticket_sla_policy

), ticket_priority_history as (

    select *
    from __dbt__CTE__stg_zendesk_ticket_field_history
    where field_name = 'priority'

), sla_policy_historical as (

    select *
    from __dbt__CTE__sla_policy_historical

), joined as (

    select 

        ticket_sla_policy.*,
        ticket_priority_history.value as ticket_priority,
        sla_policy_historical.business_hours,
        sla_policy_historical.metric, 
        sla_policy_historical.priority, 
        sla_policy_historical.target
    
    from ticket_sla_policy
    
    join ticket_priority_history
        on ticket_sla_policy.ticket_id = ticket_priority_history.ticket_id
        and (timestamp_diff(ticket_sla_policy.policy_applied_at, ticket_priority_history.valid_starting_at, second) between -2 and 0
            or ticket_sla_policy.policy_applied_at >= ticket_priority_history.valid_starting_at) -- there can be a 1-2 second diff so accounting for that
        and ticket_sla_policy.policy_applied_at < ticket_priority_history.valid_ending_at
    
    join sla_policy_historical
        on ticket_sla_policy.sla_policy_id = sla_policy_historical.sla_policy_id
        and ticket_sla_policy.policy_applied_at >= sla_policy_historical.valid_starting_at
        and ticket_sla_policy.policy_applied_at < sla_policy_historical.valid_ending_at
        and ticket_priority_history.value = sla_policy_historical.priority
)

select * 
from joined