with ticket_schedule as (

  select *
  from {{ ref('stg_zendesk_ticket_schedule') }}

), ticket as (

  select *
  from {{ ref('stg_zendesk_ticket') }}

)

select 
  ticket.ticket_id,
  coalesce(ticket_schedule.schedule_id, {{ var('default_schedule_id') }} ) as schedule_id,
  coalesce(ticket_schedule.created_at, ticket.created_at) as schedule_created_at,
  coalesce(lead(
                ticket_schedule.created_at) over (partition by ticket.ticket_id order by ticket_schedule.created_at)
          , timestamp("9999-12-31 01:01:01")) as schedule_invalidated_at
from ticket

left join ticket_schedule
  on ticket.ticket_id = ticket_schedule.ticket_id