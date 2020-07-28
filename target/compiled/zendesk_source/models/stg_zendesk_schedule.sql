

with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`schedule`

), fields as (
    
    select

      id as schedule_id,
      end_time_utc,
      start_time_utc,
      name as schedule_name
      
    from base
    where not _fivetran_deleted

)

select *
from fields