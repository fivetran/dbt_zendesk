with requester_wait_time_sla as (

  select *
  from {{ ref('int_zendesk__sla_policy_applied') }}
  where metric = 'requester_wait_time'

), ticket_historical_status as (

  select *
  from {{ ref('int_zendesk__ticket_historical_status') }}
    
--This captures the statuses of the ticket while the requester wait time sla was active for the ticket.
), requester_wait_time_filtered_statuses as (

  select  
    ticket_historical_status.ticket_id,
    greatest(ticket_historical_status.valid_starting_at, requester_wait_time_sla.sla_applied_at) as valid_starting_at,
    coalesce(
      ticket_historical_status.valid_ending_at, 
      {{ fivetran_utils.timestamp_add('day', 30, "" ~ dbt.current_timestamp_backcompat() ~ "") }} ) as valid_ending_at, --assumes current status continues into the future. This is necessary to predict future SLA breaches (not just past).
    ticket_historical_status.status as ticket_status,
    requester_wait_time_sla.sla_applied_at,
    requester_wait_time_sla.target,
    requester_wait_time_sla.sla_policy_name,
    requester_wait_time_sla.ticket_created_at,
    requester_wait_time_sla.in_business_hours
  from ticket_historical_status
  join requester_wait_time_sla
    on ticket_historical_status.ticket_id = requester_wait_time_sla.ticket_id
  where ticket_historical_status.status in ('new', 'open', 'on-hold', 'hold') -- these are the only statuses that count as "requester wait time"
  and sla_applied_at < valid_ending_at

)
select *
from requester_wait_time_filtered_statuses