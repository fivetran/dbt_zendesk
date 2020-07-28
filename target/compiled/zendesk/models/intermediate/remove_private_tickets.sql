-- view of tickets that are created with private comments.  This is needed as a condition for determining first reply time calculations
with  __dbt__CTE__stg_zendesk_ticket_field_history as (
with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`ticket_field_history`

), fields as (
    
    select
    
      ticket_id,
      field_name,
      updated as valid_starting_at,
      lead(updated) over (partition by ticket_id, field_name order by updated) as valid_ending_at,
      value,
      user_id

    from base
    order by 1,2,3

)

select *
from fields
),  __dbt__CTE__stg_zendesk_ticket as (
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
),ticket_field_history as (

    select *
    from __dbt__CTE__stg_zendesk_ticket_field_history
    where field_name = 'is_public'


), ticket as (

    select *
    from __dbt__CTE__stg_zendesk_ticket

), joined as (

    select
        
        ticket.ticket_id,
        ticket_field_history.valid_ending_at < current_timestamp() as was_made_public,
        case when ticket_field_history.valid_ending_at < current_timestamp() 
            then ticket_field_history.valid_ending_at 
            else null end as made_public_at

    from ticket_field_history

    join ticket 
        on ticket.ticket_id = ticket_field_history.ticket_id
        and ticket.created_at = ticket_field_history.valid_starting_at
        and ticket_field_history.value = '0'

)
select *
from joined