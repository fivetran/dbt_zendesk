with  __dbt__CTE__stg_zendesk_ticket_schedule as (


with base as (

    select *
    from `zendesk`.`ticket_schedule`

), fields as (
    
    select

      ticket_id,
      created_at,
      schedule_id,
      
    from base

)

select *
from fields
),  __dbt__CTE__stg_zendesk_ticket as (
with base as (

    select *
    from `zendesk`.`ticket`

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
),ticket_schedule as (

  select *
  from __dbt__CTE__stg_zendesk_ticket_schedule

), ticket as (

  select *
  from __dbt__CTE__stg_zendesk_ticket

)

select 
  ticket.ticket_id,
  coalesce(ticket_schedule.schedule_id, 15574 ) as schedule_id,
  coalesce(ticket_schedule.created_at, ticket.created_at) as schedule_created_at,
  coalesce(lead(
                ticket_schedule.created_at) over (partition by ticket.ticket_id order by ticket_schedule.created_at)
          , timestamp("9999-12-31 01:01:01")) as schedule_invalidated_at
from ticket

left join ticket_schedule
  on ticket.ticket_id = ticket_schedule.ticket_id