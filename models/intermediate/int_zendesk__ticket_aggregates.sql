with tickets as (
  select *
  from {{ ref('stg_zendesk__ticket') }}

), ticket_tags as (

  select *
  from {{ ref('stg_zendesk__ticket_tag') }}

), brands as (

  select *
  from {{ ref('stg_zendesk__brand') }}
  
), ticket_tag_aggregate as (
  select
    ticket_tags.ticket_id,
    {{ fivetran_utils.string_agg( 'ticket_tags.tags', "', '" )}} as ticket_tags
  from ticket_tags
  group by 1

), final as (
  select 
    tickets.*,
    case when lower(tickets.type) = 'incident'
      then true
      else false
        end as is_incident,
    brands.name as ticket_brand_name,
    ticket_tag_aggregate.ticket_tags
  from tickets

  left join ticket_tag_aggregate
    using(ticket_id)

  left join brands
    on brands.brand_id = tickets.brand_id
)

select *
from final