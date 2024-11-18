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
    source_relation,
    {{ fivetran_utils.string_agg( 'user_tags.tags', "', '" )}} as user_tags
  from user_tags
  group by 1, 2

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
    on users.user_id = user_tag_aggregate.user_id 
    and users.source_relation = user_tag_aggregate.source_relation
  {% endif %}
)

select *
from final