{{ config(enabled=var('using_sla_policy', True)) }}

--REPLY TIME BREACH
-- step 2, figure out when the sla will breach for sla's in calendar hours. the calculation is relatively straightforward.

with sla_policy_applied as (

  select *
  from {{ ref('int_zendesk__sla_policy_applied') }}

)

  select
    *,
    {{ timestamp_add(
        "minute",
        "cast(target as " ~ dbt_utils.type_int() ~ " )",
        "sla_applied_at" ) }} as breached_at
  from sla_policy_applied
  where in_business_hours = 'false'
    and metric in ('next_reply_time', 'first_reply_time')
