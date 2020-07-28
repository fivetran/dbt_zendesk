

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