
{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

-- The necessary source and source_filter adjustments used below originate from the int_zendesk__sla_policy_applied model
with source as (
    select
        *,
        case when field_name = 'first_reply_time' then row_number() over (partition by ticket_id, field_name order by valid_starting_at) else 1 end as latest_sla
    from {{ ref('stg_zendesk__ticket_field_history') }}
),

source_filter as (
    select
        ticket_id,
        count(*) as source_row_count
    from source
    where field_name in ('next_reply_time', 'first_reply_time', 'agent_work_time', 'requester_wait_time')
        and value is not null
        and latest_sla = 1
    group by 1
),

sla_policies as (
    select
        ticket_id,
        count(*) as end_model_row_count
    from {{ ref('zendesk__sla_policies') }}
    group by 1
),

match_check as (
    select 
        sla_policies.ticket_id,
        end_model_row_count,
        source_row_count
    from sla_policies
    full outer join source_filter
        on source_filter.ticket_id = sla_policies.ticket_id
)

select *
from match_check
where end_model_row_count != source_row_count
{{ "and ticket_id not in " ~ var('fivetran_integrity_sla_count_match_tickets',[]) ~ "" if var('fivetran_integrity_sla_count_match_tickets',[]) }}