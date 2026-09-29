--final step where we union together all of the reply time, agent work time, and requester wait time sla's

-- is_sla_paused is constant per ticket (it's evaluated against ticket-level fields only), so a
-- single row per ticket is enough to override is_sla_breach for every metric below.
with ticket_sla_pause as (

  select distinct
    source_relation,
    ticket_id,
    is_sla_paused
  from {{ ref('int_zendesk__sla_policy_applied') }}

), reply_time_sla as (

  select *
  from {{ ref('int_zendesk__reply_time_combined') }}

), agent_work_calendar_sla as (

  select *
  from {{ ref('int_zendesk__agent_work_time_calendar_hours') }}

), requester_wait_calendar_sla as (

  select *
  from {{ ref('int_zendesk__requester_wait_time_calendar_hours') }}

{% if var('using_schedules', True) %}

), agent_work_business_sla as (

  select *
  from {{ ref('int_zendesk__agent_work_time_business_hours') }}

), requester_wait_business_sla as (
  select *
  from {{ ref('int_zendesk__requester_wait_time_business_hours') }}

{% endif %}

), all_slas_unioned as (
  select
    source_relation,
    ticket_id,
    sla_policy_name,
    metric,
    sla_applied_at,
    target,
    in_business_hours,
    priority_applied,
    sla_update_at as sla_breach_at,
    sla_elapsed_time,
    is_sla_breached
  from reply_time_sla

union all

  select
    source_relation,
    ticket_id,
    sla_policy_name,
    'agent_work_time' as metric,
    sla_applied_at,
    target,
    false as in_business_hours,
    priority_applied,
    max(sla_breach_at) as sla_breach_at,
    max(running_total_calendar_minutes) as sla_elapsed_time,
    {{ fivetran_utils.max_bool("is_breached_during_schedule") }} as is_sla_breached
  from agent_work_calendar_sla

  {{ dbt_utils.group_by(n=8) }}

union all

  select
    source_relation,
    ticket_id,
    sla_policy_name,
    'requester_wait_time' as metric,
    sla_applied_at,
    target,
    false as in_business_hours,
    priority_applied,
    max(sla_breach_at) as sla_breach_at,
    max(running_total_calendar_minutes) as sla_elapsed_time,
    {{ fivetran_utils.max_bool("is_breached_during_schedule") }} as is_sla_breached
  from requester_wait_calendar_sla

  {{ dbt_utils.group_by(n=8) }}


{% if var('using_schedules', True) %}

union all 

  select
    source_relation,
    ticket_id,
    sla_policy_name,
    'agent_work_time' as metric,
    sla_applied_at,
    target,
    true as in_business_hours,
    priority_applied,
    max(sla_breach_at) as sla_breach_at,
    max(running_total_scheduled_minutes) as sla_elapsed_time,
    {{ fivetran_utils.max_bool("is_breached_during_schedule") }} as is_sla_breached
  from agent_work_business_sla

  {{ dbt_utils.group_by(n=8) }}

union all 

  select
    source_relation,
    ticket_id,
    sla_policy_name,
    'requester_wait_time' as metric,
    sla_applied_at,
    target,
    true as in_business_hours,
    priority_applied,
    max(sla_breach_at) as sla_breach_at,
    max(running_total_scheduled_minutes) as sla_elapsed_time,
    {{ fivetran_utils.max_bool("is_breached_during_schedule") }} as is_sla_breached

  from requester_wait_business_sla

  {{ dbt_utils.group_by(n=8) }}

{% endif %}

)

select
  {{ dbt_utils.generate_surrogate_key(['all_slas_unioned.source_relation', 'all_slas_unioned.ticket_id', 'all_slas_unioned.metric', 'all_slas_unioned.sla_applied_at']) }} as sla_event_id,
  all_slas_unioned.source_relation,
  all_slas_unioned.ticket_id,
  all_slas_unioned.sla_policy_name,
  all_slas_unioned.metric,
  all_slas_unioned.sla_applied_at,
  all_slas_unioned.target,
  all_slas_unioned.in_business_hours,
  all_slas_unioned.priority_applied,
  all_slas_unioned.sla_breach_at,
  case when all_slas_unioned.sla_elapsed_time is null
    then round(cast(({{ dbt.datediff("all_slas_unioned.sla_applied_at", dbt.current_timestamp(), 'second') }} / 60) as {{ dbt.type_numeric() }}), 4)  --This will create an entry for active sla's
    else round(cast(all_slas_unioned.sla_elapsed_time as {{ dbt.type_numeric() }}), 4)
      end as sla_elapsed_time,
  all_slas_unioned.sla_breach_at > current_timestamp as is_active_sla,
  case when (all_slas_unioned.sla_breach_at > {{ dbt.current_timestamp() }})
    then null
    when coalesce(ticket_sla_pause.is_sla_paused, false)
    then false
    else all_slas_unioned.is_sla_breached
      end as is_sla_breach
from all_slas_unioned
left join ticket_sla_pause
  on ticket_sla_pause.ticket_id = all_slas_unioned.ticket_id
  and ticket_sla_pause.source_relation = all_slas_unioned.source_relation