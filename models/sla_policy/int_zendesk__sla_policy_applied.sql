-- step 1, figure out when sla was applied to tickets

-- more on SLA policies here: https://support.zendesk.com/hc/en-us/articles/204770038-Defining-and-using-SLA-policies-Professional-and-Enterprise-
-- SLA policies are calculated for next_reply_time, first_reply_time, agent_work_time, requester_wait_time.  If you're company uses other SLA metrics, and would like this
-- package to support those, please reach out to the Fivetran team on Slack.

{% set check_sla_policy_metric_history = var('using_sla_policy_metric_history', True) and var('using_ticket_sla_policy', True) %}

with ticket_field_history as (

  select *
  from {{ ref('int_zendesk__updates') }}

), sla_policy_name as (

  select 
    *
  from {{ ref('int_zendesk__updates') }}
  where field_name = ('sla_policy')

), ticket as (

  select *
  from {{ ref('int_zendesk__ticket_aggregates') }}

), comments_enriched as (

  select *
  from {{ ref('int_zendesk__comments_enriched') }}

-- The customer's first comment, public or private, so we can tell if they've engaged at all.
), first_external_comment as (

  select
    source_relation,
    ticket_id,
    min(valid_starting_at) as first_external_comment_at
  from comments_enriched
  where commenter_role = 'external_comment'
  {{ dbt_utils.group_by(n=2) }}

), private_ticket_creation as (
  -- Flags tickets created via private comments where the customer hasn't engaged yet, so
  -- first_reply_time can start at their first public comment instead of ticket_created_at
  -- (mirrors int_zendesk__ticket_reply_times.sql).
  select
    comments_enriched.source_relation,
    comments_enriched.ticket_id,
    max(case when comments_enriched.previous_commenter_role = 'first_comment'
          and comments_enriched.commenter_role = 'internal_comment'
          and comments_enriched.previous_internal_comment_count > 0
          and (first_external_comment.first_external_comment_at is null
            or comments_enriched.valid_starting_at < first_external_comment.first_external_comment_at)
        then 1 else 0 end) = 1 as is_privately_created,
    min(case when comments_enriched.commenter_role = 'external_comment' then comments_enriched.valid_starting_at end) as first_customer_public_comment_at
  from comments_enriched
  left join first_external_comment
    on first_external_comment.ticket_id = comments_enriched.ticket_id
    and first_external_comment.source_relation = comments_enriched.source_relation
  where comments_enriched.is_public
  {{ dbt_utils.group_by(n=2) }}

{% if check_sla_policy_metric_history %}
), sla_policy_metrics as (

    select *
    from {{ ref('stg_zendesk__sla_policy_metric_history') }}

), ticket_sla_policy as (

    select *
    from {{ ref('stg_zendesk__ticket_sla_policy') }}

{% endif %}

), sla_policy_applied as (

  select
    ticket_field_history.source_relation,
    ticket_field_history.ticket_id,
    ticket.created_at as ticket_created_at,
    ticket_field_history.valid_starting_at,
    ticket.status as ticket_current_status,
    ticket_field_history.field_name as metric,
    case when ticket_field_history.field_name = 'first_reply_time' then row_number() over (partition by ticket_field_history.ticket_id, ticket_field_history.field_name {{ fivetran_utils.partition_by_source_relation(package_name='zendesk', alias='ticket_field_history') }} order by ticket_field_history.valid_starting_at desc) else 1 end as latest_sla,
    case
      when ticket_field_history.field_name = 'first_reply_time' and coalesce(private_ticket_creation.is_privately_created, false)
        then coalesce(private_ticket_creation.first_customer_public_comment_at, ticket.created_at)
      when ticket_field_history.field_name = 'first_reply_time'
        then ticket.created_at
      else ticket_field_history.valid_starting_at
    end as sla_applied_at,
    cast({{ fivetran_utils.json_parse('ticket_field_history.value', ['minutes']) }} as {{ dbt.type_int() }} ) as target,
    {{ fivetran_utils.json_parse('ticket_field_history.value', ['in_business_hours']) }} = 'true' as in_business_hours,
    ticket.priority as current_priority,
    ticket_field_history.field_name = 'first_reply_time' and coalesce(private_ticket_creation.is_privately_created, false) as is_privately_created
  from ticket_field_history
  join ticket
    on ticket.ticket_id = ticket_field_history.ticket_id
    and ticket.source_relation = ticket_field_history.source_relation
  left join private_ticket_creation
    on private_ticket_creation.ticket_id = ticket_field_history.ticket_id
    and private_ticket_creation.source_relation = ticket_field_history.source_relation
  where ticket_field_history.value is not null
    and ticket_field_history.field_name in ('next_reply_time', 'first_reply_time', 'agent_work_time', 'requester_wait_time')

), add_sla_policy_name as (

  select
    sla_policy_applied.*,
    sla_policy_name.value as sla_policy_name
  from sla_policy_applied
  left join sla_policy_name
    on sla_policy_name.ticket_id = sla_policy_applied.ticket_id
    and sla_policy_name.source_relation = sla_policy_applied.source_relation
      and {{ dbt.date_trunc("second", "sla_policy_applied.valid_starting_at") }} >= {{ dbt.date_trunc("second", "sla_policy_name.valid_starting_at") }}
      and {{ dbt.date_trunc("second", "sla_policy_applied.valid_starting_at") }} < coalesce({{ dbt.date_trunc("second", "sla_policy_name.valid_ending_at") }}, {{ dbt.current_timestamp() }})
  where sla_policy_applied.latest_sla = 1

), ticket_priority_history as (

    select
        ticket_id,
        source_relation,
        valid_starting_at,
        valid_ending_at,
        value as priority
    from {{ ref('int_zendesk__updates') }}
    where field_name = 'priority'

), add_historical_priority as (

    select
        add_sla_policy_name.*,
        coalesce(ticket_priority_history.priority, add_sla_policy_name.current_priority) as priority_applied
    from add_sla_policy_name
    left join ticket_priority_history
        on add_sla_policy_name.ticket_id = ticket_priority_history.ticket_id
        and add_sla_policy_name.source_relation = ticket_priority_history.source_relation
        and {{ dbt.date_trunc("second", "add_sla_policy_name.sla_applied_at") }} >= {{ dbt.date_trunc("second", "ticket_priority_history.valid_starting_at") }}
        and {{ dbt.date_trunc("second", "add_sla_policy_name.sla_applied_at") }} < coalesce({{ dbt.date_trunc("second", "ticket_priority_history.valid_ending_at") }}, {{ dbt.current_timestamp() }})

{% if check_sla_policy_metric_history %}
), add_sla_policy_id as (

    -- Zendesk re-logs the currently-applied SLA policy on most ticket updates, so a ticket can have many
    -- `ticket_sla_policy` rows. An exact timestamp match against sla_applied_at misses cases where none
    -- of those log entries land on that exact moment, so instead take whichever policy was most recently
    -- applied at or before sla_applied_at.
    select
        add_historical_priority.*,
        ticket_sla_policy.sla_policy_id,
        row_number() over (
            partition by add_historical_priority.source_relation, add_historical_priority.ticket_id, add_historical_priority.metric, add_historical_priority.valid_starting_at
            order by ticket_sla_policy.policy_applied_at desc
        ) as policy_rank
    from add_historical_priority
    left join ticket_sla_policy
        on add_historical_priority.ticket_id = ticket_sla_policy.ticket_id
        and add_historical_priority.source_relation = ticket_sla_policy.source_relation
        and ticket_sla_policy.policy_applied_at <= add_historical_priority.sla_applied_at
{% endif %}

), final as (

{% if check_sla_policy_metric_history %}

    select
      add_sla_policy_id.source_relation,
      add_sla_policy_id.ticket_id,
      add_sla_policy_id.ticket_created_at,
      add_sla_policy_id.valid_starting_at,
      add_sla_policy_id.ticket_current_status,
      add_sla_policy_id.metric,
      add_sla_policy_id.latest_sla,
      add_sla_policy_id.sla_applied_at,
      coalesce(sla_policy_metrics.target, add_sla_policy_id.target) as target,
      add_sla_policy_id.in_business_hours,
      add_sla_policy_id.current_priority,
      add_sla_policy_id.priority_applied,
      add_sla_policy_id.sla_policy_name,
      add_sla_policy_id.is_privately_created

    from add_sla_policy_id
    left join sla_policy_metrics
      on add_sla_policy_id.metric = sla_policy_metrics.metric
      and add_sla_policy_id.sla_policy_id = sla_policy_metrics.sla_policy_id
      and add_sla_policy_id.priority_applied = sla_policy_metrics.priority
      and add_sla_policy_id.source_relation = sla_policy_metrics.source_relation
      and add_sla_policy_id.sla_applied_at >= sla_policy_metrics.valid_starting_at
      and add_sla_policy_id.sla_applied_at < coalesce(sla_policy_metrics.valid_ending_at, {{ dbt.current_timestamp() }})
    where add_sla_policy_id.policy_rank = 1

{% else %}

  select *
  from add_historical_priority
{% endif %}
)

select *
from final