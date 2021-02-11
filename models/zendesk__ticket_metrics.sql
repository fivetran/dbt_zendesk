with ticket_enriched as (

  select *
  from {{ ref('zendesk__ticket_enriched') }}

), ticket_resolution_times_calendar as (

  select *
  from {{ ref('int_zendesk__ticket_resolution_times_calendar') }}

), ticket_reply_times_calendar as (

  select *
  from {{ ref('int_zendesk__ticket_reply_times_calendar') }}

), ticket_one_touch_resolution as (

  select *
  from {{ ref('int_zendesk__ticket_one_touch_resolution') }}

), ticket_work_time_calendar as (

  select *
  from {{ ref('int_zendesk__ticket_work_time_calendar') }}

-- business hour CTEs
{% if var('using_schedules', True) %}

), ticket_first_resolution_time_business as (

  select *
  from {{ ref('int_zendesk__ticket_first_resolution_time_business') }}

), ticket_full_resolution_time_business as (

  select *
  from {{ ref('int_zendesk__ticket_full_resolution_time_business') }}

), ticket_work_time_business as (

  select *
  from {{ ref('int_zendesk__ticket_work_time_business') }}

), ticket_first_reply_time_business as (

  select *
  from {{ ref('int_zendesk__ticket_first_reply_time_business') }}

{% endif %}
-- end business hour CTEs

), calendar_hour_metrics as (

select
  ticket_enriched.*,
  ticket_reply_times_calendar.first_reply_time_calendar_minutes,
  ticket_reply_times_calendar.total_reply_time_calendar_minutes,
  ticket_resolution_times_calendar.first_solved_at,
  ticket_resolution_times_calendar.last_solved_at,
  ticket_resolution_times_calendar.first_resolution_calendar_minutes,
  ticket_resolution_times_calendar.final_resolution_calendar_minutes,
  ticket_work_time_calendar.agent_wait_time_in_calendar_minutes,
  ticket_work_time_calendar.requester_wait_time_in_calendar_minutes,
  ticket_work_time_calendar.agent_work_time_in_calendar_minutes,
  ticket_work_time_calendar.on_hold_time_in_calendar_minutes,
  ticket_one_touch_resolution.count_internal_comments as total_agent_replies,
  
  case when ticket_enriched.status in ('solved','closed') and ticket_one_touch_resolution.is_one_touch_resolution then true
    else false end as is_one_touch_resolution,
  case when ticket_enriched.status in ('solved','closed') and ticket_one_touch_resolution.is_two_touch_resolution then true
    else false end as is_two_touch_resolution,
  case when ticket_enriched.status in ('solved','closed') and not ticket_one_touch_resolution.is_one_touch_resolution then true
    else false end as is_multi_touch_resolution


from ticket_enriched

left join ticket_reply_times_calendar
  using (ticket_id)

left join ticket_resolution_times_calendar
  using (ticket_id)

left join ticket_one_touch_resolution
  using (ticket_id)

left join ticket_work_time_calendar
  using (ticket_id)

{% if var('using_schedules', True) %}

), business_hour_metrics as (

  select 
    ticket_enriched.ticket_id,
    ticket_first_resolution_time_business.first_resolution_business_minutes,
    ticket_full_resolution_time_business.full_resolution_business_minutes,
    ticket_first_reply_time_business.first_reply_time_business_minutes,
    ticket_work_time_business.agent_wait_time_in_business_minutes,
    ticket_work_time_business.requester_wait_time_in_business_minutes,
    ticket_work_time_business.agent_work_time_in_business_minutes,
    ticket_work_time_business.on_hold_time_in_business_minutes

  from ticket_enriched

  left join ticket_first_resolution_time_business
    using (ticket_id)

  left join ticket_full_resolution_time_business
    using (ticket_id)
  
  left join ticket_first_reply_time_business
    using (ticket_id)  
  
  left join ticket_work_time_business
    using (ticket_id)

)

select
  calendar_hour_metrics.*,
  business_hour_metrics.first_resolution_business_minutes,
  business_hour_metrics.full_resolution_business_minutes

from calendar_hour_metrics

left join business_hour_metrics 
  using (ticket_id)

{% else %}

) 

select *
from calendar_hour_metrics

{% endif %}