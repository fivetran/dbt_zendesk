with users as (
    select *
    from {{ ref('int_zendesk__user_aggregates') }}

), organizations as (
    select *
    from {{ ref('int_zendesk__organization_aggregates') }}

), final as (
    select
        users.user_id as updater_user_id, 
        users.name as updater_name, 
        users.role as updater_role, 
        users.email as updater_email, 
        users.external_id as updater_external_id, 
        users.locale as updater_locale, 
        users.is_active as updater_is_active, 
        users.user_tags as updater_user_tags, 
        users.last_login_at as updater_last_login_at, 
        users.time_zone as updater_time_zone, 
        organizations.organization_id as updater_organization_id, 
        organizations.domain_names as updater_organization_domain_names, 
        organizations.organization_tags as updater_organization_organization_tags
    from users

    left join organizations
        using(organization_id)
)

select * 
from final