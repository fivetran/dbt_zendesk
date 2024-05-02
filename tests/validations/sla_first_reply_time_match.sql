{{ config(
    tags="integrity",
    enabled=var('fivetran_zendesk_validation_tests_enabled', false)
) }}

with ticket_metrics as (
    select
        ticket_id,
        first_reply_time_business_minutes
    from {{ ref('zendesk__ticket_metrics') }}
),

sla_policies as (
    select
        ticket_id,
        sla_elapsed_time
    from {{ ref('zendesk__sla_policies') }}
    where metric = 'first_reply_time'
),

match_check as (
    select 
        ticket_metrics.ticket_id,
        ticket_metrics.first_reply_time_business_minutes,
        sla_policies.sla_elapsed_time
    from ticket_metrics
    full outer join sla_policies 
        on ticket_metrics.ticket_id = sla_policies.ticket_id
    where abs(round(ticket_metrics.first_reply_time_business_minutes,0) - round(sla_policies.sla_elapsed_time,0)) >= 2
        {{ "and ticket_metrics.ticket_id not in " ~ var('fivetran_zendesk_exclusion_tickets',[]) ~ "" if var('fivetran_zendesk_exclusion_tickets',[]) }}
)

select *
from match_check