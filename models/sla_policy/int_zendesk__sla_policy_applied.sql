-- step 1, figure out when sla was applied to tickets

-- more on SLA policies here: https://support.zendesk.com/hc/en-us/articles/204770038-Defining-and-using-SLA-policies-Professional-and-Enterprise-
-- SLA policies are calculated for next_reply_time, first_reply_time, agent_work_time, requester_wait_time.  If you're company uses other SLA metrics, and would like this
-- package to support those, please reach out to the Fivetran team on Slack.

with ticket_field_history as (

  select *
  from {{ ref('int_zendesk__updates') }}

), sla_policy_name as (

  select *
  from {{ ref('int_zendesk__updates') }}
  where field_name = ('sla_policy')

), ticket as (

  select *
  from {{ ref('int_zendesk__ticket_aggregates') }}

), sla_policy_applied as (

  select
    ticket_field_history.ticket_id,
    ticket.created_at as ticket_created_at,
    ticket.status as ticket_current_status,
    ticket_field_history.field_name as metric,
    case when ticket_field_history.field_name = 'first_reply_time' then ticket.created_at else ticket_field_history.valid_starting_at end as sla_applied_at,
    cast({{ fivetran_utils.json_extract('ticket_field_history.value', 'minutes') }} as {{ dbt_utils.type_int() }} ) as target,
    {{ fivetran_utils.json_extract('ticket_field_history.value', 'in_business_hours') }} = 'true' as in_business_hours
  from ticket_field_history
  join ticket
    on ticket.ticket_id = ticket_field_history.ticket_id
  where ticket_field_history.value is not null
    and ticket_field_history.field_name in ('next_reply_time', 'first_reply_time', 'agent_work_time', 'requester_wait_time')

), final as (
  select
    sla_policy_applied.*,
    sla_policy_name.value as sla_policy_name
  from sla_policy_applied
  left join sla_policy_name
    on sla_policy_name.ticket_id = sla_policy_applied.ticket_id
      and sla_policy_applied.sla_applied_at >= sla_policy_name.valid_starting_at
      and sla_policy_applied.sla_applied_at < coalesce(sla_policy_name.valid_ending_at, {{ dbt_utils.current_timestamp() }}) 
)

select *
from final