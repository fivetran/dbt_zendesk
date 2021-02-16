with organizations as (
    select * 
    from {{ ref('stg_zendesk__organization') }}

), organization_tags as (
    select * 
    from {{ ref('stg_zendesk__organization_tag') }}

), domain_names as (

    select *
    from {{ ref('stg_zendesk__domain_name') }}

), org_aggregates as (
    select 
        organizations.organization_id,
        {{ fivetran_utils.string_agg('organization_tags.tags', "', '" ) }} as organization_tags,
        {{ fivetran_utils.string_agg('domain_names.domain_name', "', '" ) }} as domain_names
    from organizations

    left join organization_tags
        using (organization_id)

    left join domain_names
        using(organization_id)
    
    group by 1

), final as (
    select
        organizations.*,
        org_aggregates.organization_tags,
        org_aggregates.domain_names
    from organizations

    left join org_aggregates
        using(organization_id)
)

select *
from final