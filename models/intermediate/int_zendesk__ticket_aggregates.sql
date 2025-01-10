with tickets as (
  select *
  from {{ ref('stg_zendesk__ticket') }}

), ticket_tags as (

  select *
  from {{ ref('stg_zendesk__ticket_tag') }}

--If you use using_brands this will be included, if not it will be ignored.
{% if var('using_brands', True) %}
), brands as (

  select *
  from {{ ref('stg_zendesk__brand') }}
{% endif %}
  
), ticket_tag_aggregate as (
  select
    source_relation,
    ticket_tags.ticket_id,
    {{ fivetran_utils.string_agg( 'ticket_tags.tags', "', '" )}} as ticket_tags
  from ticket_tags
  group by 1, 2

), final as (
  select 
    tickets.*,
    case when lower(tickets.type) = 'incident'
      then true
      else false
        end as is_incident,
    {% if var('using_brands', True) %}
    brands.name as ticket_brand_name,
    {% endif %}
    ticket_tag_aggregate.ticket_tags
  from tickets

  left join ticket_tag_aggregate
    on tickets.ticket_id = ticket_tag_aggregate.ticket_id 
    and tickets.source_relation = ticket_tag_aggregate.source_relation

  {% if var('using_brands', True) %}
  left join brands
    on brands.brand_id = tickets.brand_id
    and brands.source_relation = tickets.source_relation
  {% endif %}    
)

select *
from final