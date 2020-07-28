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
),  __dbt__CTE__ticket_historical_status as (
with ticket_status_history as (

    select *
    from __dbt__CTE__stg_zendesk_ticket_field_history
    where field_name = 'status'

)

  select
  
    ticket_id,
    valid_starting_at,
    valid_ending_at,
    timestamp_diff(coalesce(valid_ending_at,current_timestamp()),valid_starting_at, minute) as status_duration_calendar_minutes,
    value as status,
    row_number() over (partition by ticket_id order by valid_starting_at) as ticket_status_counter,
    row_number() over (partition by ticket_id, value order by valid_starting_at) as unique_status_counter

  from ticket_status_history
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
),historical_solved_status as (

    select *
    from __dbt__CTE__ticket_historical_status
    where status = 'solved'

), ticket as (

    select *
    from __dbt__CTE__stg_zendesk_ticket

), solved_times as (
  
  select
  
    ticket_id,
    min(valid_starting_at) as first_solved_at,
    max(valid_starting_at) as last_solved_at

  from historical_solved_status
  group by 1

)

  select

    ticket.ticket_id,
    ticket.created_at,
    solved_times.first_solved_at,
    solved_times.last_solved_at,
    timestamp_diff(solved_times.first_solved_at,ticket.created_at, minute) as first_resolution_calendar_minutes,
    timestamp_diff(solved_times.last_solved_at,ticket.created_at, minute) as final_resolution_calendar_minutes

  from ticket
  left join solved_times
    on solved_times.ticket_id = ticket.ticket_id