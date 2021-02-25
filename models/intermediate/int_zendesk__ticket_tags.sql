with ticket_tags as (

    select *
    from {{ ref('stg_zendesk__ticket_tag') }}
  
)

select
  ticket_tags.ticket_id,
  {{ fivetran_utils.string_agg( 'ticket_tags.tags', "', '" )}} as ticket_tags
from ticket_tags
group by 1