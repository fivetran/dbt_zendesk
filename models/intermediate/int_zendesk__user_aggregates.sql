with users as (
  select *
  from {{ ref('stg_zendesk__user') }}

--If you use user tags this will be included, if not it will be ignored.
{% if var('using_user_tags', True) %}
), user_tags as (

  select *
  from {{ ref('stg_zendesk__user_tag') }}
  
), user_tag_aggregate as (
  select
    user_tags.user_id,
    {{ fivetran_utils.string_agg( 'user_tags.tags', "', '" )}} as user_tags
  from user_tags
  group by 1

{% endif %}

), final as (
  select 
    users.*

    --If you use user tags this will be included, if not it will be ignored.
    {% if var('using_user_tags', True) %}
    ,user_tag_aggregate.user_tags
    {% endif %}
  from users

  --If you use user tags this will be included, if not it will be ignored.
  {% if var('using_user_tags', True) %}
  left join user_tag_aggregate
    using(user_id)
  {% endif %}
)

select *
from final