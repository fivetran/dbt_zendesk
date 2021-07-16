with ticket_enriched as (

  select *
  from {{ ref('zendesk__ticket_enriched') }}

), ticket_resolution_times_calendar as (

  select *
  from {{ ref('int_zendesk__ticket_resolution_times_calendar') }}

), ticket_reply_times_calendar as (

  select *
  from {{ ref('int_zendesk__ticket_reply_times_calendar') }}

), ticket_comments as (

  select *
  from {{ ref('int_zendesk__comment_metrics') }}

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
  case when coalesce(ticket_comments.count_public_agent_comments, 0) = 0 and ticket_enriched.status = 'solved'
    then null
    else ticket_reply_times_calendar.first_reply_time_calendar_minutes
      end as first_reply_time_calendar_minutes,
  case when coalesce(ticket_comments.count_public_agent_comments, 0) = 0 and ticket_enriched.status = 'solved'
    then null
    else ticket_reply_times_calendar.total_reply_time_calendar_minutes
      end as total_reply_time_calendar_minutes,
  coalesce(ticket_comments.count_agent_comments, 0) as count_agent_comments,
  coalesce(ticket_comments.count_public_agent_comments, 0) as count_public_agent_comments,
  coalesce(ticket_comments.count_end_user_comments, 0) as count_end_user_comments,
  coalesce(ticket_comments.count_public_comments, 0) as count_public_comments,
  coalesce(ticket_comments.count_internal_comments, 0) as count_internal_comments,
  coalesce(ticket_comments.total_comments, 0) as total_comments,
  ticket_comments.last_comment_added_at as ticket_last_comment_date,
  ticket_resolution_times_calendar.unique_assignee_count,
  ticket_resolution_times_calendar.assignee_stations_count,
  ticket_resolution_times_calendar.group_stations_count,
  ticket_resolution_times_calendar.first_assignee_id,
  ticket_resolution_times_calendar.last_assignee_id,
  ticket_resolution_times_calendar.first_agent_assignment_date,
  ticket_resolution_times_calendar.last_agent_assignment_date,
  ticket_resolution_times_calendar.first_solved_at,
  ticket_resolution_times_calendar.last_solved_at,
  case when ticket_enriched.status != 'solved' 
    then null
    else ticket_resolution_times_calendar.first_assignment_to_resolution_calendar_minutes
      end as first_assignment_to_resolution_calendar_minutes,
  case when ticket_enriched.status != 'solved'
    then null
    else ticket_resolution_times_calendar.last_assignment_to_resolution_calendar_minutes
      end as last_assignment_to_resolution_calendar_minutes,
  ticket_resolution_times_calendar.ticket_unassigned_duration_calendar_minutes,
  ticket_resolution_times_calendar.first_resolution_calendar_minutes,
  ticket_resolution_times_calendar.final_resolution_calendar_minutes,
  ticket_resolution_times_calendar.total_resolutions as count_resolutions,
  ticket_resolution_times_calendar.count_reopens,
  ticket_work_time_calendar.ticket_deleted_count,
  ticket_work_time_calendar.total_ticket_recoveries,
  ticket_work_time_calendar.last_status_assignment_date,
  ticket_work_time_calendar.new_status_duration_in_calendar_minutes,
  ticket_work_time_calendar.open_status_duration_in_calendar_minutes,
  ticket_work_time_calendar.agent_wait_time_in_calendar_minutes,
  ticket_work_time_calendar.requester_wait_time_in_calendar_minutes,
  ticket_work_time_calendar.agent_work_time_in_calendar_minutes,
  ticket_work_time_calendar.on_hold_time_in_calendar_minutes,
  ticket_comments.count_internal_comments as total_agent_replies,
  
  case when ticket_enriched.is_requester_active = true and ticket_enriched.requester_last_login_at is not null
    then ({{ dbt_utils.datediff("ticket_enriched.requester_last_login_at", dbt_utils.current_timestamp(), 'second') }} /60)
      end as requester_last_login_age_minutes,
  case when ticket_enriched.is_assignee_active = true and ticket_enriched.assignee_last_login_at is not null
    then ({{ dbt_utils.datediff("ticket_enriched.assignee_last_login_at", dbt_utils.current_timestamp(), 'second') }} /60)
      end as assignee_last_login_age_minutes,
  case when lower(ticket_enriched.status) not in ('solved','closed')
    then ({{ dbt_utils.datediff("ticket_enriched.created_at", dbt_utils.current_timestamp(), 'second') }} /60)
      end as unsolved_ticket_age_minutes,
  case when lower(ticket_enriched.status) not in ('solved','closed')
    then ({{ dbt_utils.datediff("ticket_enriched.updated_at", dbt_utils.current_timestamp(), 'second') }} /60)
      end as unsolved_ticket_age_since_update_minutes,
  case when lower(ticket_enriched.status) in ('solved','closed') and ticket_comments.is_one_touch_resolution 
    then true
    else false
      end as is_one_touch_resolution,
  case when lower(ticket_enriched.status) in ('solved','closed') and ticket_comments.is_two_touch_resolution 
    then true
    else false 
      end as is_two_touch_resolution,
  case when lower(ticket_enriched.status) in ('solved','closed') and not ticket_comments.is_one_touch_resolution 
    then true
    else false 
      end as is_multi_touch_resolution


from ticket_enriched

left join ticket_reply_times_calendar
  using (ticket_id)

left join ticket_resolution_times_calendar
  using (ticket_id)

left join ticket_work_time_calendar
  using (ticket_id)

left join ticket_comments
  using(ticket_id)

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
  business_hour_metrics.full_resolution_business_minutes,
  case when calendar_hour_metrics.status = 'solved' and calendar_hour_metrics.count_public_agent_comments = 0
    then null
    else business_hour_metrics.first_reply_time_business_minutes
      end as first_reply_time_business_minutes,
  business_hour_metrics.agent_wait_time_in_business_minutes,
  business_hour_metrics.requester_wait_time_in_business_minutes,
  business_hour_metrics.agent_work_time_in_business_minutes,
  business_hour_metrics.on_hold_time_in_business_minutes

from calendar_hour_metrics

left join business_hour_metrics 
  using (ticket_id)

{% else %}

) 

select *
from calendar_hour_metrics

{% endif %}
