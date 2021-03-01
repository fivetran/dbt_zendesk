--To disable this model, set the using_user_tags variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_user_tags', True)) }}

with user_tags as (

    select *
    from {{ ref('stg_zendesk__user_tag') }}
  
)

select
  user_tags.user_id,
  {{ fivetran_utils.string_agg( 'user_tags.tags', "', '" )}} as tags
from user_tags
group by 1