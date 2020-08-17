-- this model enriches the ticket table with ticket-related dimensions.  This table will not include any metrics.
-- for metrics, see ticket_metrics!

with ticket as (

    select *
    from {{ ref('stg_zendesk_ticket') }}

), user as (

    select *
    from {{ ref('stg_zendesk_user') }}

), ticket_group as (
    
    select *
    from {{ ref('stg_zendesk_group') }}

), organization as (

    select *
    from {{ ref('stg_zendesk_organization') }}

), ticket_tags as (

    select *
    from {{ ref('ticket_tags') }}

), joined as (

    select 

        ticket.*,
        requester.role as requester_role,
        requester.email as requester_email,
        requester.name as requester_name,
        submitter.role as submitter_role,
        submitter.email as submitter_email,
        submitter.name as submitter_name,
        assignee.role as assignee_role,
        assignee.email as assignee_email,
        assignee.name as assignee_name,
        ticket_group.name as group_name,
        organization.name as organization_name,
        ticket_tags.ticket_tags

    
    from ticket

    join user as requester
        on requester.user_id = ticket.requester_id
    
    join user as submitter
        on submitter.user_id = ticket.submitter_id
    
    left join user as assignee
        on assignee.user_id = ticket.assignee_id

    left join ticket_group
        on ticket_group.group_id = ticket.group_id

    left join organization
        on organization.organization_id = ticket.organization_id

    left join ticket_tags
        on ticket_tags.ticket_id = ticket.ticket_id
)

select *
from joined