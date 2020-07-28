

with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`ticket_schedule`

), fields as (
    
    select

      ticket_id,
      created_at,
      schedule_id,
      
    from base

)

select *
from fields