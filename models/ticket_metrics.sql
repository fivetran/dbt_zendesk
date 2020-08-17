with tickets_enhanced as (

  select *
  from {{ ref('tickets_enhanced') }}

), ticket_resolution_times_calendar as (

  select *
  from {{ ref('ticket_resolution_times_calendar') }}

), ticket_reply_times_calendar as (

  select *
  from {{ ref('ticket_reply_times_calendar') }}

), ticket_one_touch_resolution as (

  select *
  from {{ ref('ticket_one_touch_resolution') }}

-- business hour CTEs
{% if var('using_schedules', True) %}

), ticket_first_resolution_time_business as (

  select *
  from {{ ref('ticket_first_resolution_time_business') }}

), ticket_full_resolution_time_business as (

  select *
  from {{ ref('ticket_full_resolution_time_business') }}

{% endif %}
-- end business hour CTEs

), calendar_hour_metrics as (

select
  tickets_enhanced.*,
  ticket_resolution_times_calendar.first_solved_at,
  ticket_resolution_times_calendar.last_solved_at,
  ticket_resolution_times_calendar.first_resolution_calendar_minutes,
  ticket_resolution_times_calendar.final_resolution_calendar_minutes,
  ticket_reply_times_calendar.first_reply_time_calendar_minutes,
  ticket_reply_times_calendar.total_reply_time_calendar_minutes,
  case when tickets_enhanced.status in ('solved','closed') and is_one_touch_resolution then true
    else false end as is_one_touch_resolution


from tickets_enhanced

left join ticket_resolution_times_calendar
  using (ticket_id)

left join ticket_reply_times_calendar
  using (ticket_id)

left join ticket_one_touch_resolution
  using (ticket_id)

{% if var('using_schedules', True) %}

), business_hour_metrics as (

  select 
    tickets_enhanced.ticket_id,
    ticket_first_resolution_time_business.first_resolution_business_minutes,
    ticket_full_resolution_time_business.full_resolution_business_minutes,

  from tickets_enhanced

  left join ticket_first_resolution_time_business
    using (ticket_id)

  left join ticket_full_resolution_time_business
    using (ticket_id)

)

select
  calendar_hour_metrics.*,
  business_hour_metrics.first_resolution_business_minutes,
  business_hour_metrics.full_resolution_business_minutes,

from calendar_hour_metrics

left join business_hour_metrics 
  using (ticket_id)

{% else %}

) 

select *
from calendar_hour_metrics

{% endif %}