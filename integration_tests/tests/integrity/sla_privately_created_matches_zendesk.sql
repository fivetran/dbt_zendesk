{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

-- For tickets we flag as privately-created (shifting first_reply_time's clock to the customer's
-- engagement, per int_zendesk__sla_policy_applied.is_privately_created), Zendesk's own
-- ticket_field_history should show it delaying first_reply_time too -- not applying it right at
-- ticket creation. If Zendesk's earliest first_reply_time entry for a ticket is still within a
-- few minutes of ticket_created_at, Zendesk did not actually delay this ticket (e.g. it's a
-- follow-up ticket we haven't identified, or some other pattern we don't yet know about), and our
-- is_privately_created flag disagrees with what Zendesk itself did -- is_sla_breach for this
-- ticket may not match Zendesk's real reporting. See the follow-up-ticket case in DECISIONLOG.md
-- for the first example of this; this test exists to catch the next one proactively instead of
-- waiting for a support report.

with privately_created_tickets as (

    select distinct
        source_relation,
        ticket_id,
        ticket_created_at
    from {{ ref('int_zendesk__sla_policy_applied') }}
    where metric = 'first_reply_time'
        and is_privately_created

), first_reply_time_history as (

    select
        ticket_id,
        source_relation,
        min(valid_starting_at) as first_applied_at
    from {{ ref('stg_zendesk__ticket_field_history') }}
    where field_name = 'first_reply_time'
    {{ dbt_utils.group_by(n=2) }}

)

select
    privately_created_tickets.source_relation,
    privately_created_tickets.ticket_id,
    privately_created_tickets.ticket_created_at,
    first_reply_time_history.first_applied_at
from privately_created_tickets
join first_reply_time_history
    on first_reply_time_history.ticket_id = privately_created_tickets.ticket_id
    and first_reply_time_history.source_relation = privately_created_tickets.source_relation
where {{ dbt.datediff('privately_created_tickets.ticket_created_at', 'first_reply_time_history.first_applied_at', 'minute') }} <= 5
    {{ "and privately_created_tickets.ticket_id not in " ~ var('fivetran_integrity_sla_privately_created_exclusion_tickets',[]) ~ "" if var('fivetran_integrity_sla_privately_created_exclusion_tickets',[]) }}
