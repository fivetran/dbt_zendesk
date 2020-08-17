{{ config(enabled=var('using_sla_policy', True)) }}

--final step where we union together all of the reply time and agent work time breaches

with reply_time_breach as (

  select * 
  from {{ ref('reply_time_breach_combined') }}

), agent_work_calendar_breach as (

  select *
  from {{ ref('agent_work_time_calendar_hours_breached') }}

{% if var('using_schedules', True) %}

), agent_work_business_breach as (

  select *
  from {{ ref('agent_work_time_business_hours_breached') }}

{% endif %}

), all_breaches_unioned as (
  select
    ticket_id,
    metric,
    sla_applied_at,
    target,
    in_business_hours,
    breached_at
  from reply_time_breach

union all

  select
    ticket_id,
    'agent_work_time' as metric,
    sla_applied_at,
    target,
    'false' as in_business_hours,
    breached_at
  from agent_work_calendar_breach

{% if var('using_schedules', True) %}

union all 

  select 
    ticket_id,
    'agent_work_time' as metric,
    sla_applied_at,
    target,
    'true' as in_business_hours,
    breached_at
  from agent_work_business_breach

{% endif %}

)

select 
  *,
  breached_at > current_timestamp as is_upcoming_breach
from all_breaches_unioned