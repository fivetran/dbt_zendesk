--REPLY TIME SLA
-- step 2, figure out when the sla will breach for sla's in calendar hours. The calculation is relatively straightforward.
{{ config(enabled=var('customer360__using_zendesk', true)) }}

with sla_policy_applied as (

  select *
  from {{ ref('int_zendesk__sla_policy_applied') }}

), final as (
  select
    *,
    {{ fivetran_utils.timestamp_add(
        "minute",
        "cast(target as " ~ dbt.type_int() ~ " )",
        "sla_applied_at" ) }} as sla_breach_at
  from sla_policy_applied
  where not in_business_hours
    and metric in ('next_reply_time', 'first_reply_time')

)

select *
from final
