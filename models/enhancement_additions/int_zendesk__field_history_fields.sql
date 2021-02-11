with ticket_field_history as (

  select *
  from {{ ref('zendesk__ticket_field_history') }}

),

first_last_ticket as (
    select 
        ticket_id,
        min(date_day) as ticket_first_assigned,
        max
    from ticket_field_history
)