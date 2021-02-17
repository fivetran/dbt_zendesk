-- this model enriches the ticket table with ticket-related dimensions.  This table will not include any metrics.
-- for metrics, see ticket_metrics!

with ticket as (

    select *
    from {{ ref('stg_zendesk__ticket') }}

), latest_ticket_form as (

    select *
    from {{ ref('int_zendesk__latest_ticket_form') }}

), latest_satisfaction_ratings as (

    select *
    from {{ ref('int_zendesk__latest_satisfaction_rating') }}

), users as (

    select *
    from {{ ref('stg_zendesk__user') }}

), requester_last_update as (

    select *
    from {{ ref('int_zendesk__requester_last_update') }}

), assignee_last_update as (

    select *
    from {{ ref('int_zendesk__assignee_last_update') }}

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
    from {{ ref('int_zendesk__organization_aggregates') }}

), ticket_tags as (

    select *
    from {{ ref('int_zendesk__ticket_tags') }}

), joined as (

    select 

        ticket.*,
        case when lower(ticket.type) = 'incident'
            then true
            else false
                end as is_incident,
        brands.name as ticket_brand_name,
        latest_ticket_form.name as ticket_form_name,
        organization.domain_names as ticket_organization_domain_names,
        latest_satisfaction_ratings.score as ticket_satisfaction_rating,
        latest_satisfaction_ratings.comment as ticket_satisfaction_comment,
        latest_satisfaction_ratings.reason as ticket_satisfaction_reason,
        requester.external_id as requester_external_id,
        requester.created_at as requester_created_at,
        requester.updated_at as requester_updated_at,
        requester.role as requester_role,
        requester.email as requester_email,
        requester.name as requester_name,
        requester.is_active as is_requester_active,
        requester_tag.tags as requester_tag,
        requester.locale as requester_locale,
        requester.time_zone as requester_time_zone,
        requester_last_update.last_updated as requester_ticket_last_update_at,
        requester.last_login_at as requester_last_login_at,
        requester.organization_id as requester_organization_id,
        requester_org.name as requester_organization_name,
        requester_org.domain_names as requester_organization_domain_names,

        --If you use organization tags this will be included, if not it will be ignored.
        {% if var('using_organization_tags', True) %}
        requester_org.organization_tags as requester_organization_tags,
        {% endif %}

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
        submitter.is_active as is_submitter_active,
        submitter_tag.tags as submitter_tag,
        submitter.locale as submitter_locale,
        submitter.time_zone as submitter_time_zone,
        assignee.external_id as assignee_external_id,
        assignee.role as assignee_role,
        assignee.email as assignee_email,
        assignee.name as assignee_name,
        assignee.is_active as is_assignee_active,
        assignee_tag.tags as assignee_tag,
        assignee.locale as assignee_locale,
        assignee.time_zone as assignee_time_zone,
        assignee_last_update.last_updated as assigneed_ticket_last_update_at,
        assignee.last_login_at as assignee_last_login_at,
        ticket_group.name as group_name,
        organization.name as organization_name,
        ticket_tags.ticket_tags

    
    from ticket

    --Requester Joins
    join users as requester
        on requester.user_id = ticket.requester_id
    
    left join user_tags as requester_tag
        on requester_tag.user_id = ticket.requester_id

    left join organization as requester_org
        on requester_org.organization_id = requester.organization_id

    left join requester_last_update
        on requester_last_update.ticket_id = ticket.ticket_id
            and requester_last_update.requester_id = ticket.requester_id
    
    --Submitter Joins
    join users as submitter
        on submitter.user_id = ticket.submitter_id

    left join user_tags as submitter_tag
        on submitter_tag.user_id = ticket.submitter_id
    
    --Assignee Joins
    left join users as assignee
        on assignee.user_id = ticket.assignee_id

    left join user_tags as assignee_tag
        on assignee_tag.user_id = ticket.assignee_id

    left join assignee_last_update
        on assignee_last_update.ticket_id = ticket.ticket_id
            and assignee_last_update.assignee_id = ticket.assignee_id

    --Ticket, Org, and Brand Joins
    left join ticket_group
        on ticket_group.group_id = ticket.group_id

    left join latest_ticket_form
        on latest_ticket_form.ticket_form_id = ticket.ticket_form_id

    left join brands
        on brands.brand_id = ticket.brand_id

    left join organization
        on organization.organization_id = ticket.organization_id

    left join latest_satisfaction_ratings
        on latest_satisfaction_ratings.ticket_id = ticket.ticket_id

    left join ticket_tags
        on ticket_tags.ticket_id = ticket.ticket_id
)

select *
from joined