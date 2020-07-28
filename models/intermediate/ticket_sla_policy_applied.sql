{{ config(enabled=var('using_sla_policy', True)) }}

with ticket_sla_policy as (

    select *
    from {{ ref('stg_zendesk_ticket_sla_policy') }}

), ticket_priority_history as (

    select *
    from {{ ref('stg_zendesk_ticket_field_history') }}
    where field_name = 'priority'

), sla_policy_historical as (

    select *
    from {{ ref('sla_policy_historical') }}

), joined as (

    select 

        ticket_sla_policy.*,
        ticket_priority_history.value as ticket_priority,
        sla_policy_historical.business_hours,
        sla_policy_historical.metric, 
        sla_policy_historical.priority, 
        sla_policy_historical.target
    
    from ticket_sla_policy
    
    join ticket_priority_history
        on ticket_sla_policy.ticket_id = ticket_priority_history.ticket_id
        and (timestamp_diff(ticket_sla_policy.policy_applied_at, ticket_priority_history.valid_starting_at, second) between -2 and 0
            or ticket_sla_policy.policy_applied_at >= ticket_priority_history.valid_starting_at) -- there can be a 1-2 second diff so accounting for that
        and ticket_sla_policy.policy_applied_at < ticket_priority_history.valid_ending_at
    
    join sla_policy_historical
        on ticket_sla_policy.sla_policy_id = sla_policy_historical.sla_policy_id
        and ticket_sla_policy.policy_applied_at >= sla_policy_historical.valid_starting_at
        and ticket_sla_policy.policy_applied_at < sla_policy_historical.valid_ending_at
        and ticket_priority_history.value = sla_policy_historical.priority
)

select * 
from joined