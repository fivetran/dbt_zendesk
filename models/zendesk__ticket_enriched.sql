-- this model enriches the ticket table with ticket-related dimensions.  This table will not include any metrics.
-- for metrics, see ticket_metrics!

with ticket as (

    select *
    from {{ ref('stg_zendesk__ticket') }}

), latest_ticket_form as (

    select *
    from {{ ref('int_zendesk__latest_ticket_form') }}

), satisfaction_ratings as (

    select *
    from {{ ref('stg_zendesk__satisfaction_rating') }}

), users as (

    select *
    from {{ ref('stg_zendesk__user') }}

), brands as (

    select *
    from {{ ref('stg_zendesk__brand') }}

), user_tags as (

    select *
    from {{ ref('stg_zendesk__user_tag') }}

), ticket_group as (
    
    select *
    from {{ ref('stg_zendesk__group') }}

), organization as (

    select *
    from {{ ref('stg_zendesk__organization') }}

), organization_tags as (

    select *
    from {{ ref('int_zendesk__organization_tag_agg') }}

), domain_names as (

    select *
    from {{ ref('stg_zendesk__domain_name') }}

), ticket_tags as (

    select *
    from {{ ref('int_zendesk__ticket_tags') }}

), joined as (

    select 

        ticket.*,
        brands.name as ticket_brand_name,
        latest_ticket_form.name as ticket_form_name,
        ticket_org_domain.domain_name as ticket_organization_domain_name,
        satisfaction_ratings.score as ticket_satisfaction_rating,
        satisfaction_ratings.comment as ticket_satisfaction_comment,
        satisfaction_ratings.reason as ticket_satisfaction_reason,
        requester.external_id as requester_external_id,
        requester.created_at as requester_created_at,
        requester.updated_at as requester_updated_at,
        requester.role as requester_role,
        requester.email as requester_email,
        requester.name as requester_name,
        requester_tag.tags as requester_tag,
        requester.locale as requester_locale,
        requester.time_zone as requester_time_zone,
        requester.last_login_at as requester_last_login_at,
        requester.organization_id as requester_organization_id,
        requester_org.name as requester_organization_name,
        requester_org_domain.domain_name as requester_organization_domain_name,
        requester_org_tag.organization_tags as requester_organization_tags,           --This field is a string_agg
        requester_org.external_id as requester_organization_external_id,
        requester_org.created_at as requester_organization_created_at,
        requester_org.updated_at as requester_organization_updated_at,
        submitter.external_id as submitter_external_id,
        submitter.role as submitter_role,
        case when submitter.role in ('Agent','Admin') 
            then true 
            else false 
                end as is_agent_submitted,
        submitter.email as submitter_email,
        submitter.name as submitter_name,
        submitter_tag.tags as submitter_tag,
        submitter.locale as submitter_locale,
        submitter.time_zone as submitter_time_zone,
        assignee.external_id as assignee_external_id,
        assignee.role as assignee_role,
        assignee.email as assignee_email,
        assignee.name as assignee_name,
        assignee_tag.tags as assignee_tag,
        assignee.locale as assignee_locale,
        assignee.time_zone as assignee_time_zone,
        assignee.last_login_at as assignee_last_login_at,
        ticket_group.name as group_name,
        organization.name as organization_name,
        ticket_tags.ticket_tags

    
    from ticket

    --Requester Joins
    join users as requester
        on requester.user_id = ticket.requester_id
    
    join user_tags as requester_tag
        on requester_tag.user_id = ticket.requester_id

    left join organization as requester_org
        on requester_org.organization_id = requester.organization_id
    
    left join domain_names as requester_org_domain
        on requester_org_domain.organization_id = requester_org.organization_id

    left join organization_tags as requester_org_tag
        on requester_org_tag.organization_id = requester_org.organization_id
    
    --Submitter Joins
    join users as submitter
        on submitter.user_id = ticket.submitter_id

    join user_tags as submitter_tag
        on submitter_tag.user_id = ticket.submitter_id
    
    --Assignee Joins
    left join users as assignee
        on assignee.user_id = ticket.assignee_id

    left join user_tags as assignee_tag
        on assignee_tag.user_id = ticket.assignee_id

    --Ticket, Org, and Brand Joins
    left join ticket_group
        on ticket_group.group_id = ticket.group_id

    left join latest_ticket_form
        on latest_ticket_form.ticket_form_id = ticket.ticket_form_id

    left join brands
        on brands.brand_id = ticket.brand_id

    left join organization
        on organization.organization_id = ticket.organization_id

    left join organization_tags as ticket_org_tag
        on ticket_org_tag.organization_id = organization.organization_id

    left join domain_names as ticket_org_domain
        on ticket_org_domain.organization_id = organization.organization_id

    left join satisfaction_ratings
        on satisfaction_ratings.ticket_id = ticket.ticket_id

    left join ticket_tags
        on ticket_tags.ticket_id = ticket.ticket_id
)

select *
from joined