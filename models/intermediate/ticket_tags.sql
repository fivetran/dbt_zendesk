with ticket_tags as (

    select *
    from {{ ref('stg_zendesk_ticket_tag') }}
  
)

select
  ticket_tags.ticket_id,
  {{ string_agg( 'ticket_tags.tag', "', '" )}} as ticket_tags
from ticket_tags
group by 1