with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`group`
    
), fields as (

    select

      id as group_id,
      name

    from base
    where not _fivetran_deleted


)

select *
from fields