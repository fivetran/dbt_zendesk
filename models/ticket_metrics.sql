with tickets_enhanced as (

  select *
  from {{ ref('tickets_enhanced') }}

), ticket_resolution_times_calendar as (

  select *
  from {{ ref('ticket_resolution_times_calendar') }}


--if using schedules

), ticket_first_resolution_time_business as (

  select *
  from {{ ref('ticket_first_resolution_time_business') }}

), ticket_full_resolution_time_business as (

  select *
  from {{ ref('ticket_full_resolution_time_business') }}

),



select
  tickets_enhanced.*,
  ticket_resolution_times_calendar.first_solved_at,
  ticket_resolution_times_calendar.last_solved_at,
  ticket_resolution_times_calendar.first_resolution_calendar_minutes,
  ticket_resolution_times_calendar.final_resolution_calendar_minutes,


---if using schedules
  ticket_first_resolution_time_business.first_resolution_business_minutes,
  ticket_full_resolution_time_business.first_resolution_business_minutes,

from tickets_enhanced

left join ticket_resolution_times_calendar
  on tickets_enhanced.ticket_id = ticket_resolution_times_calendar.ticket_id

---if using schedules

left join ticket_first_resolution_time_business
  on ticket_first_resolution_time_business.ticket_id = tickets_enhanced.ticket_id

left join ticket_full_resolution_time_business
  on ticket_full_resolution_time_business.ticket_id = tickets_enhanced.ticket_id
