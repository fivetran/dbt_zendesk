
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

comments_enriched as (
    select *
    from {{ ref('int_zendesk__comments_enriched') }}
),

first_external_comment as (
    select
        source_relation,
        ticket_id,
        min(valid_starting_at) as first_external_comment_at
    from comments_enriched
    where commenter_role = 'external_comment'
    {{ dbt_utils.group_by(n=2) }}
),

-- Tickets created via private comments where the customer hadn't engaged yet: zendesk__sla_policies
-- anchors first_reply_time to the customer's first public comment for these, but zendesk__ticket_metrics
-- intentionally does not (see DECISIONLOG.md), so they're expected to diverge and are excluded here.
privately_created_tickets as (
    select
        comments_enriched.source_relation,
        comments_enriched.ticket_id
    from comments_enriched
    left join first_external_comment
        on first_external_comment.ticket_id = comments_enriched.ticket_id
        and first_external_comment.source_relation = comments_enriched.source_relation
    where comments_enriched.is_public
        and comments_enriched.previous_commenter_role = 'first_comment'
        and comments_enriched.commenter_role = 'internal_comment'
        and comments_enriched.previous_internal_comment_count > 0
        and (first_external_comment.first_external_comment_at is null
            or comments_enriched.valid_starting_at < first_external_comment.first_external_comment_at)
    group by 1, 2
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