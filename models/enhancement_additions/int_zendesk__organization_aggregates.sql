with organizations as (
    select * 
    from {{ ref('stg_zendesk__organization') }}

--If you use organization tags this will be included, if not it will be ignored.
{% if var('using_organization_tags', True) %}
), organization_tags as (
    select * 
    from {{ ref('stg_zendesk__organization_tag') }}

), tag_aggregates as (
    select
        organizations.organization_id,
        {{ fivetran_utils.string_agg('organization_tags.tags', "', '" ) }} as organization_tags
    from organizations

    left join organization_tags
        using (organization_id)

    group by 1
{% endif %}

), domain_names as (

    select *
    from {{ ref('stg_zendesk__domain_name') }}

), domain_aggregates as (
    select
        organizations.organization_id,
        {{ fivetran_utils.string_agg('domain_names.domain_name', "', '" ) }} as domain_names
    from organizations

    left join domain_names
        using(organization_id)
    
    group by 1

), final as (
    select
        organizations.*,

        --If you use organization tags this will be included, if not it will be ignored.
        {% if var('using_organization_tags', True) %}
        tag_aggregates.organization_tags,
        {% endif %}
        
        domain_aggregates.domain_names
    from organizations

    left join domain_aggregates
        using(organization_id)

    --If you use organization tags this will be included, if not it will be ignored.
    {% if var('using_organization_tags', True) %}
    left join tag_aggregates
        using(organization_id)
    {% endif %}
)

select *
from final