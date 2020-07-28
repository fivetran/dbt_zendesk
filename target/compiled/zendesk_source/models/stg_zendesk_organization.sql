with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`organization`

), fields as (

    select

      id as organization_id,
      details,
      name

    from base

)

select *
from fields