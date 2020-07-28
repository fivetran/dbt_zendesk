



select count(*) as validation_errors
from (

    select
        id

    from `digital-arbor-400`.`zendesk`.`ticket`
    where id is not null
    group by id
    having count(*) > 1

) validation_errors

