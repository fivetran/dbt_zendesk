with ticket_field_history as (
  select *
  from {{ ref('stg_zendesk__ticket_field_history') }}
  
), 

count_updates as (
    select
        ticket_id,
        count(*) as total_updates
    from ticket_field_history

    group by 1
)

select *
from count_updates
