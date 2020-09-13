{{ config(enabled=var('using_sla_policy', True)) }}

-- step 1, figure out when sla was applied to tickets


-- more on SLA policies here: https://support.zendesk.com/hc/en-us/articles/204770038-Defining-and-using-SLA-policies-Professional-and-Enterprise-
-- SLA policies are calculated for next_reply_time, first_reply_time and agent_work_time.  If you're company uses other SLA metrics, and would like this
-- package to support those, please reach out to the Fivetran team on Slack.

with ticket_field_history as (

  select *
  from {{ ref('stg_zendesk_ticket_field_history') }}

), ticket as (

  select *
  from {{ ref('stg_zendesk_ticket') }}

), sla_policy_applied as (

  select
    ticket_field_history.ticket_id,
    ticket.created_at as ticket_created_at,
    ticket.status as ticket_current_status,
    ticket_field_history.field_name as metric,
    ticket_field_history.valid_starting_at as sla_applied_at,
    cast({{ json_extract('ticket_field_history.value', 'minutes') }} as {{ dbt_utils.type_int() }} ) as target,
    {{ json_extract('ticket_field_history.value', 'in_business_hours') }} as in_business_hours
  from ticket_field_history
  join ticket
    on ticket.ticket_id = ticket_field_history.ticket_id
  where ticket_field_history.value is not null
    and ticket_field_history.field_name in ('next_reply_time', 'first_reply_time', 'agent_work_time')

)
select *
from sla_policy_applied