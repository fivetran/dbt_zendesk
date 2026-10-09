
{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

with ticket_metrics as (
    select
        ticket_id,
        source_relation,
        first_reply_time_business_minutes
    from {{ ref('zendesk__ticket_metrics') }}
),

sla_policies as (
    select
        ticket_id,
        source_relation,
        sla_elapsed_time
    from {{ ref('zendesk__sla_policies') }}
    where metric = 'first_reply_time'
        and in_business_hours
),

-- Exclude tickets where Zendesk delayed first_reply_time past ticket creation: sla_policies tracks that delay, ticket_metrics intentionally doesn't (see DECISIONLOG.md), so the two are expected to diverge here.
privately_created_tickets as (
    select distinct
        source_relation,
        ticket_id
    from {{ ref('int_zendesk__sla_policy_applied') }}
    where metric = 'first_reply_time'
        and {{ dbt.date_trunc("second", "sla_applied_at") }} != {{ dbt.date_trunc("second", "ticket_created_at") }}
),

match_check as (
    select
        coalesce(ticket_metrics.source_relation, sla_policies.source_relation) as source_relation,
        coalesce(ticket_metrics.ticket_id, sla_policies.ticket_id) as ticket_id,
        ticket_metrics.first_reply_time_business_minutes,
        sla_policies.sla_elapsed_time
    from ticket_metrics
    full outer join sla_policies
        on ticket_metrics.ticket_id = sla_policies.ticket_id
        and ticket_metrics.source_relation = sla_policies.source_relation
)

select match_check.*
from match_check
left join privately_created_tickets
    on privately_created_tickets.ticket_id = match_check.ticket_id
    and privately_created_tickets.source_relation = match_check.source_relation
where abs(round(match_check.first_reply_time_business_minutes,0) - round(match_check.sla_elapsed_time,0)) >= 2
    and privately_created_tickets.ticket_id is null
    {{ "and match_check.ticket_id not in " ~ var('fivetran_integrity_sla_first_reply_time_exclusion_tickets',[]) ~ "" if var('fivetran_integrity_sla_first_reply_time_exclusion_tickets',[]) }}