with organization_tags as (
    select * 
    from {{ ref('stg_zendesk__organization_tag') }}
),

org_tag_aggregates as (
    select 
        organization_id,
        {{ fivetran_utils.string_agg('tags', "', '" ) }} as organization_tags
    from organization_tags
    
    group by 1
)

select *
from org_tag_aggregates