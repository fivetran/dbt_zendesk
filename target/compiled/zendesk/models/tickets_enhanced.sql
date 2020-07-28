with  __dbt__CTE__stg_zendesk_ticket as (
with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`ticket`

), fields as (

    select

      id as ticket_id,
      _fivetran_synced,
      assignee_id,
      brand_id,
      created_at,
      description,
      due_at,
      group_id,
      is_public,
      organization_id,
      priority,
      recipient,
      requester_id,
      status,
      subject,
      submitter_id,
      ticket_form_id,
      type,
      updated_at,
      url,
      via_channel as created_channel,
      via_source_from_id as source_from_id,
      via_source_from_title as source_from_title,
      via_source_rel as source_rel,
      via_source_to_address as source_to_address,
      via_source_to_name as source_to_name

    from base

)

select *
from fields
),  __dbt__CTE__stg_zendesk_user as (
with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`user`

), fields as (

    select

      id as user_id,
      _fivetran_synced,
      created_at,
      email,
      name,
      organization_id,
      role,
      ticket_restriction,
      time_zone,
      active as is_active

    from base

)

select *
from fields
),ticket as (

    select *
    from __dbt__CTE__stg_zendesk_ticket

), user as (

    select *
    from __dbt__CTE__stg_zendesk_user

), joined as (

    select 

        ticket.*,
        requester.role as requester_role,
        requester.role = 'agent' as agent_created_ticket,
        requester.email as requester_email,
        submitter.role as submitter_role,
        submitter.email as submitter_email,

    
    from ticket

    join user as requester
        on requester.user_id = ticket.requester_id
    
    join user as submitter
        on submitter.user_id = ticket.submitter_id
)

select *
from joined